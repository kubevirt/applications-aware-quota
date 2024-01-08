/*
 * This file is part of the MTQ project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2023,Red Hat, Inc.
 *
 */
package mtq_controller

import (
	"context"
	"fmt"
	"github.com/emicklei/go-restful/v3"
	"io/ioutil"
	k8sv1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	v14 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"
	"kubevirt.io/kubevirt/pkg/certificates/bootstrap"
	"kubevirt.io/kubevirt/pkg/virt-controller/leaderelectionconfig"
	"kubevirt.io/managed-tenant-quota/pkg/generated/clientset/versioned/typed/core/v1alpha1"
	"kubevirt.io/managed-tenant-quota/pkg/mtq-controller/vmmrq-controller"
	"kubevirt.io/managed-tenant-quota/pkg/mtq-operator/resources/namespaced"
	"kubevirt.io/managed-tenant-quota/pkg/util"
	"os"
	"strconv"

	golog "log"
	"net/http"
)

type MtqControllerApp struct {
	ctx                   context.Context
	mtqNs                 string
	host                  string
	LeaderElection        leaderelectionconfig.Configuration
	clientSet             kubecli.KubevirtClient
	mtqCli                v1alpha1.MtqV1alpha1Client
	vmmrqController       *vmmrq_controller.VmmrqController
	podInformer           cache.SharedIndexInformer
	limitRangeInformer    cache.SharedIndexInformer
	migrationInformer     cache.SharedIndexInformer
	resourceQuotaInformer cache.SharedIndexInformer
	vmmrqInformer         cache.SharedIndexInformer
	vmiInformer           cache.SharedIndexInformer
	nsInformer            cache.SharedIndexInformer
	kubeVirtInformer      cache.SharedIndexInformer
	crdInformer           cache.SharedIndexInformer
	pvcInformer           cache.SharedIndexInformer
	readyChan             chan bool
	leaderElector         *leaderelection.LeaderElector
}

func Execute() {
	var err error
	var app = MtqControllerApp{}

	app.LeaderElection = leaderelectionconfig.DefaultLeaderElectionConfiguration()
	app.readyChan = make(chan bool, 1)
	log.InitializeLogging("mtq-controller")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.ctx = ctx

	webService := new(restful.WebService)
	webService.Path("/").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)
	webService.Route(webService.GET("/leader").To(app.leaderProbe).Doc("Leader endpoint"))
	restful.Add(webService)

	virtCli, err := util.GetVirtCli()
	if err != nil {
		golog.Fatalf("unable to virtCli: %v", err)
	}
	app.clientSet = virtCli
	nsBytes, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		panic(err)
	}
	app.mtqNs = string(nsBytes)

	host, err := os.Hostname()
	if err != nil {
		golog.Fatalf("unable to get hostname: %v", err)
	}
	app.host = host

	app.mtqCli = util.GetMTQCli()
	app.podInformer = util.GetLauncherPodInformer(virtCli)
	app.limitRangeInformer = util.GetlimitRangeInformer(virtCli)
	app.vmmrqInformer = util.GetVirtualMachineMigrationResourceQuotaInformer(app.mtqCli)
	app.resourceQuotaInformer = util.GetResourceQuotaInformer(virtCli)
	app.vmiInformer = util.GetVMIInformer(virtCli)
	app.migrationInformer = util.GetMigrationInformer(virtCli)
	app.kubeVirtInformer = util.KubeVirtInformer(virtCli)
	app.crdInformer = util.CRDInformer(virtCli)
	app.pvcInformer = util.GetPersistentVolumeClaimInformer(virtCli)
	app.nsInformer = util.GetNamespaceInformer(virtCli)
	app.initVmmrqController()
	app.Run()

	klog.V(2).Infoln("MTQ controller exited")

}

func (mca *MtqControllerApp) leaderProbe(_ *restful.Request, response *restful.Response) {
	res := map[string]interface{}{}
	select {
	case _, opened := <-mca.readyChan:
		if !opened {
			res["apiserver"] = map[string]interface{}{"leader": "true"}
			if err := response.WriteHeaderAndJson(http.StatusOK, res, restful.MIME_JSON); err != nil {
				log.Log.Warningf("failed to return 200 OK reply: %v", err)
			}
			return
		}
	default:
	}
	res["apiserver"] = map[string]interface{}{"leader": "false"}
	if err := response.WriteHeaderAndJson(http.StatusOK, res, restful.MIME_JSON); err != nil {
		log.Log.Warningf("failed to return 200 OK reply: %v", err)
	}
}

func (mca *MtqControllerApp) initVmmrqController() {
	mca.vmmrqController = vmmrq_controller.NewVmmrqController(mca.clientSet,
		mca.mtqNs, mca.mtqCli,
		mca.vmiInformer,
		mca.migrationInformer,
		mca.podInformer,
		mca.limitRangeInformer,
		mca.kubeVirtInformer,
		mca.crdInformer,
		mca.pvcInformer,
		mca.resourceQuotaInformer,
		mca.vmmrqInformer,
		mca.nsInformer,
	)
}

func (mca *MtqControllerApp) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := ctx.Done()

	logger := log.Log
	secretInformer := util.GetSecretInformer(mca.clientSet, mca.mtqNs)
	go secretInformer.Run(stop)
	if !cache.WaitForCacheSync(stop, secretInformer.HasSynced) {
		os.Exit(1)
	}

	secretCertManager := bootstrap.NewFallbackCertificateManager(
		bootstrap.NewSecretCertificateManager(
			namespaced.SecretResourceName,
			mca.mtqNs,
			secretInformer.GetStore(),
		),
	)

	secretCertManager.Start()
	defer secretCertManager.Stop()

	tlsConfig := util.SetupTLS(secretCertManager)

	go func() {
		httpLogger := logger.With("service", "http")
		_ = httpLogger.Level(log.INFO).Log("action", "listening", "interface", util.DefaultHost, "port", util.DefaultPort)
		server := http.Server{
			Addr:      fmt.Sprintf("%s:%s", util.DefaultHost, strconv.Itoa(util.DefaultPort)),
			Handler:   http.DefaultServeMux,
			TLSConfig: tlsConfig,
		}
		if err := server.ListenAndServeTLS("", ""); err != nil {
			golog.Fatal(err)
		}
	}()
	if err := mca.setupLeaderElector(); err != nil {
		golog.Fatal(err)
	}
	mca.leaderElector.Run(mca.ctx)
	panic("unreachable")
}

func (mca *MtqControllerApp) setupLeaderElector() (err error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&v14.EventSinkImpl{Interface: mca.clientSet.CoreV1().Events(v1.NamespaceAll)})
	rl, err := resourcelock.New(mca.LeaderElection.ResourceLock,
		mca.mtqNs,
		"mtq-controller",
		mca.clientSet.CoreV1(),
		mca.clientSet.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      mca.host,
			EventRecorder: eventBroadcaster.NewRecorder(scheme.Scheme, k8sv1.EventSource{Component: "mtq-controller"}),
		})

	if err != nil {
		return
	}

	mca.leaderElector, err = leaderelection.NewLeaderElector(
		leaderelection.LeaderElectionConfig{
			Lock:          rl,
			LeaseDuration: mca.LeaderElection.LeaseDuration.Duration,
			RenewDeadline: mca.LeaderElection.RenewDeadline.Duration,
			RetryPeriod:   mca.LeaderElection.RetryPeriod.Duration,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: mca.onStartedLeading(),
				OnStoppedLeading: func() {
					golog.Fatal("leaderelection lost")
				},
			},
		})

	return
}

func (mca *MtqControllerApp) onStartedLeading() func(ctx context.Context) {
	return func(ctx context.Context) {
		stop := ctx.Done()
		go mca.migrationInformer.Run(stop)
		go mca.vmiInformer.Run(stop)
		go mca.kubeVirtInformer.Run(stop)
		go mca.crdInformer.Run(stop)
		go mca.pvcInformer.Run(stop)
		go mca.podInformer.Run(stop)
		go mca.limitRangeInformer.Run(stop)
		go mca.vmmrqInformer.Run(stop)
		go mca.resourceQuotaInformer.Run(stop)
		go mca.nsInformer.Run(stop)

		if !cache.WaitForCacheSync(stop,
			mca.migrationInformer.HasSynced,
			mca.vmiInformer.HasSynced,
			mca.crdInformer.HasSynced,
			mca.kubeVirtInformer.HasSynced,
			mca.podInformer.HasSynced,
			mca.limitRangeInformer.HasSynced,
			mca.resourceQuotaInformer.HasSynced,
			mca.vmmrqInformer.HasSynced,
			mca.nsInformer.HasSynced,
		) {
			log.Log.Warningf("Failed to wait for caches to sync")
		}

		go func() {
			if err := mca.vmmrqController.Run(3, stop); err != nil {
				log.Log.Warningf("error running the clone controller: %v", err)
			}
		}()
		close(mca.readyChan)
	}
}
