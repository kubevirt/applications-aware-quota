package arq_controller

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	aaq_evaluator "kubevirt.io/applications-aware-quota/pkg/aaq-controller/aaq-evaluator"
	arq_controller "kubevirt.io/applications-aware-quota/pkg/aaq-controller/aaq-gate-controller"
	rq_controller "kubevirt.io/applications-aware-quota/pkg/aaq-controller/rq-controller"
	"kubevirt.io/applications-aware-quota/pkg/client"
	"kubevirt.io/applications-aware-quota/pkg/log"
	v1alpha12 "kubevirt.io/applications-aware-quota/staging/src/kubevirt.io/applications-aware-quota-api/pkg/apis/core/v1alpha1"
	"strings"
	"time"
)

type enqueueState string

const (
	Immediate enqueueState = "Immediate"
	Forget    enqueueState = "Forget"
	BackOff   enqueueState = "BackOff"
)

type ArqController struct {
	podInformer    cache.SharedIndexInformer
	aaqjqcInformer cache.SharedIndexInformer
	aaqCli         client.AAQClient
	// A lister/getter of resource quota objects
	arqInformer cache.SharedIndexInformer
	rqInformer  cache.SharedIndexInformer
	// A list of functions that return true when their caches have synced
	arqQueue          workqueue.RateLimitingInterface
	missingUsageQueue workqueue.RateLimitingInterface
	enqueueAllQueue   workqueue.RateLimitingInterface
	// Controls full recalculation of quota usage
	resyncPeriod time.Duration
	// knows how to calculate usage
	evalRegistry   quota.Registry
	recorder       record.EventRecorder
	syncHandler    func(key string) error
	logger         klog.Logger
	stop           <-chan struct{}
	enqueueAllChan <-chan struct{}
}

func NewArqController(clientSet client.AAQClient,
	podInformer cache.SharedIndexInformer,
	arqInformer cache.SharedIndexInformer,
	rqInformer cache.SharedIndexInformer,
	aaqjqcInformer cache.SharedIndexInformer,
	calcRegistry *aaq_evaluator.AaqCalculatorsRegistry,
	stop <-chan struct{},
	enqueueAllChan <-chan struct{},
) *ArqController {
	//eventBroadcaster := record.NewBroadcaster()
	//eventBroadcaster.StartRecordingToSink(&v14.EventSinkImpl{Interface: clientSet.CoreV1().Events(v1.NamespaceAll)})

	ctrl := &ArqController{
		aaqCli:            clientSet,
		arqInformer:       arqInformer,
		rqInformer:        rqInformer,
		podInformer:       podInformer,
		aaqjqcInformer:    aaqjqcInformer,
		arqQueue:          workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "arq_primary"),
		missingUsageQueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "arq_priority"),
		enqueueAllQueue:   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "arq_enqueue_all"),
		resyncPeriod:      metav1.Duration{Duration: 5 * time.Minute}.Duration,
		//recorder:          eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: util.ControllerPodName}),
		evalRegistry:   generic.NewRegistry([]quota.Evaluator{aaq_evaluator.NewAaqEvaluator(podInformer, calcRegistry, clock.RealClock{})}),
		logger:         klog.FromContext(context.Background()),
		stop:           stop,
		enqueueAllChan: enqueueAllChan,
	}
	ctrl.syncHandler = ctrl.syncResourceQuotaFromKey

	arqInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.AddArq,
			UpdateFunc: ctrl.updateArq,
			DeleteFunc: ctrl.DeleteArq,
		},
		ctrl.resyncPeriod,
	)
	_, err := ctrl.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: ctrl.updatePod,
		AddFunc:    ctrl.AddPod,
		DeleteFunc: ctrl.DeletePod,
	})
	if err != nil {
		panic("something is wrong")
	}
	_, err = ctrl.aaqjqcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: ctrl.updateAaqjqc,
		AddFunc:    ctrl.addAaqjqc,
	})
	if err != nil {
		panic("something is wrong")
	}
	_, err = ctrl.rqInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: ctrl.updateRQ,
		AddFunc:    ctrl.addRQ,
		DeleteFunc: ctrl.deleteRQ,
	})
	if err != nil {
		panic("something is wrong")
	}

	return ctrl
}
func (ctrl *ArqController) updateRQ(old, curr interface{}) {
	curRq := curr.(*v1.ResourceQuota)
	oldRq := old.(*v1.ResourceQuota)
	if !quota.Equals(curRq.Status.Hard, oldRq.Status.Hard) || !quota.Equals(curRq.Status.Used, oldRq.Status.Used) {
		arq := &v1alpha12.ApplicationsResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Name: strings.TrimSuffix(curRq.Name, rq_controller.RQSuffix), Namespace: curRq.Namespace},
		}
		key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(arq)
		if err != nil {
			return
		}

		ctrl.arqQueue.Add(key)
	}
	return
}
func (ctrl *ArqController) deleteRQ(obj interface{}) {
	rq := obj.(*v1.ResourceQuota)
	arq := &v1alpha12.ApplicationsResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: strings.TrimSuffix(rq.Name, rq_controller.RQSuffix), Namespace: rq.Namespace},
	}
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(arq)
	if err != nil {
		return
	}
	ctrl.arqQueue.Add(key)
	return
}

func (ctrl *ArqController) addRQ(obj interface{}) {
	rq := obj.(*v1.ResourceQuota)
	arq := &v1alpha12.ApplicationsResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: strings.TrimSuffix(rq.Name, rq_controller.RQSuffix), Namespace: rq.Namespace},
	}
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(arq)
	if err != nil {
		return
	}
	ctrl.arqQueue.Add(key)
	return
}

// When a ApplicationsResourceQuotaaqjqc.Status.PodsInJobQueuea is updated, enqueue all gated pods for revaluation
func (ctrl *ArqController) updateAaqjqc(old, cur interface{}) {
	aaqjqc := cur.(*v1alpha12.AAQJobQueueConfig)
	if aaqjqc.Status.PodsInJobQueue != nil && len(aaqjqc.Status.PodsInJobQueue) > 0 {
		ctrl.addAllArqsInNamespace(aaqjqc.Namespace)
	}
	return
}

// When a ApplicationsResourceQuotaaqjqc.Status.PodsInJobQueuea is updated, enqueue all gated pods for revaluation
func (ctrl *ArqController) addAaqjqc(obj interface{}) {
	aaqjqc := obj.(*v1alpha12.AAQJobQueueConfig)
	if aaqjqc.Status.PodsInJobQueue != nil && len(aaqjqc.Status.PodsInJobQueue) > 0 {
		ctrl.addAllArqsInNamespace(aaqjqc.Namespace)
	}
	return
}

func (ctrl *ArqController) addAllArqsInNamespace(ns string) {
	arqObjs, err := ctrl.arqInformer.GetIndexer().ByIndex(cache.NamespaceIndex, ns)
	if err != nil {
		log.Log.Infof("AaqGateController: Error failed to list pod from podInformer")
	}
	found := false
	for _, arqObj := range arqObjs {
		arq := arqObj.(*v1alpha12.ApplicationsResourceQuota)
		key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(arq)
		if err != nil {
			return
		}
		found = true
		ctrl.enqueueAllQueue.Add(key)
	}
	if !found {
		ctrl.enqueueAllQueue.Add(ns + "/fake")
	}
}

// enqueueAll is called at the fullResyncPeriod interval to force a full recalculation of quota usage statistics
func (ctrl *ArqController) enqueueAll() {
	arqObjs := ctrl.arqInformer.GetIndexer().List()
	for _, arqObj := range arqObjs {
		arq := arqObj.(*v1alpha12.ApplicationsResourceQuota)
		key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(arqObj.(*v1alpha12.ApplicationsResourceQuota))
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %v", arq, err))
			continue
		}
		ctrl.arqQueue.Add(key)
	}
}
func (ctrl *ArqController) updateArq(old, curr interface{}) {
	oldArq := old.(*v1alpha12.ApplicationsResourceQuota)
	curArq := curr.(*v1alpha12.ApplicationsResourceQuota)
	if quota.Equals(oldArq.Spec.Hard, curArq.Spec.Hard) {
		return
	}
	ctrl.addQuota(ctrl.logger, curArq)
}

func (ctrl *ArqController) AddArq(obj interface{}) {
	ctrl.addQuota(ctrl.logger, obj)
}

func (ctrl *ArqController) DeleteArq(obj interface{}) {
	ctrl.enqueueArq(ctrl.logger, obj)
}

func (ctrl *ArqController) updatePod(old, curr interface{}) {
	currPod := curr.(*v1.Pod)
	oldPod := old.(*v1.Pod)
	if len(oldPod.Spec.SchedulingGates) == 0 && len(currPod.Spec.SchedulingGates) == 0 {
		ctrl.addAllArqsInNamespace(currPod.Namespace)
	}
}

func (ctrl *ArqController) AddPod(obj interface{}) {
	pod := obj.(*v1.Pod)
	ctrl.addAllArqsInNamespace(pod.Namespace)
}

func (ctrl *ArqController) DeletePod(obj interface{}) {
	pod := obj.(*v1.Pod)
	ctrl.addAllArqsInNamespace(pod.Namespace)
}

func (ctrl *ArqController) runGateWatcherWorker() {
	for ctrl.Execute() {
	}
}

func (ctrl *ArqController) Execute() bool {
	key, quit := ctrl.enqueueAllQueue.Get()
	if quit {
		return false
	}
	defer ctrl.enqueueAllQueue.Done(key)
	err, enqueueState := ctrl.execute(key.(string))

	if err != nil {
		log.Log.Infof(fmt.Sprintf("ArqController: Error with key: %v err: %v", key, err))
	}
	switch enqueueState {
	case BackOff:
		ctrl.enqueueAllQueue.AddRateLimited(key)
	case Forget:
		ctrl.enqueueAllQueue.Forget(key)
	case Immediate:
		ctrl.enqueueAllQueue.Add(key)
	}

	return true
}

func (ctrl *ArqController) execute(key string) (error, enqueueState) {
	var aaqjqc *v1alpha12.AAQJobQueueConfig
	ns, _, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err, Forget
	}
	aaqjqcObj, exists, err := ctrl.aaqjqcInformer.GetIndexer().GetByKey(ns + "/" + arq_controller.AaqjqcName)
	if err != nil {
		return err, Immediate
	} else if exists {
		aaqjqc = aaqjqcObj.(*v1alpha12.AAQJobQueueConfig).DeepCopy()
	}

	arqObjs, err := ctrl.arqInformer.GetIndexer().ByIndex(cache.NamespaceIndex, ns)
	if err != nil {
		return err, Immediate
	}

	for _, arqObj := range arqObjs {
		arq := arqObj.(*v1alpha12.ApplicationsResourceQuota).DeepCopy()
		err := ctrl.syncResourceQuota(arq)
		if err != nil {
			return err, Immediate
		}
	}

	if aaqjqc != nil {
		if res, err := ctrl.verifyPodsWithOutSchedulingGates(ns, aaqjqc.Status.PodsInJobQueue); err != nil || !res {
			return err, Immediate
		}
		if len(aaqjqc.Status.PodsInJobQueue) > 0 {
			aaqjqc.Status.PodsInJobQueue = []string{}
			_, err = ctrl.aaqCli.AAQJobQueueConfigs(ns).UpdateStatus(context.Background(), aaqjqc, metav1.UpdateOptions{})
			if err != nil {
				return err, Immediate
			}
		}
	}
	return nil, Forget
}

// CheckPodsForSchedulingGates checks that all pods in the specified namespace
// with the specified names do not have scheduling gates.
func (ctrl *ArqController) verifyPodsWithOutSchedulingGates(namespace string, podNames []string) (bool, error) {
	podList, err := ctrl.aaqCli.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return false, err
	}

	// Iterate through all pods and check for scheduling gates.
	for _, pod := range podList.Items {
		for _, name := range podNames {
			if pod.Name == name && len(pod.Spec.SchedulingGates) > 0 {
				return false, nil
			}
		}
	}

	return true, nil
}

func (ctrl *ArqController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()
	defer ctrl.arqQueue.ShutDown()
	defer ctrl.missingUsageQueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start a goroutine to listen for enqueue signals and call enqueueAll in case the configuration is changed.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ctrl.enqueueAllChan:
				log.Log.Infof("ArqController: Signal processed enqueued All")
				ctrl.enqueueAll()
			}
		}
	}()

	// the workers that chug through the quota calculation backlog
	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.worker(ctrl.arqQueue), time.Second, ctrl.stop)
		go wait.Until(ctrl.worker(ctrl.missingUsageQueue), time.Second, ctrl.stop)
		go wait.Until(ctrl.runGateWatcherWorker, time.Second, ctrl.stop)
	}
	// the timer for how often we do a full recalculation across all quotas
	if ctrl.resyncPeriod > 0 {
		go wait.Until(ctrl.enqueueAll, ctrl.resyncPeriod, ctrl.stop)
	} else {
		logger.Info("periodic quota controller resync disabled")
	}
	<-ctx.Done()

}

// worker runs a worker thread that just dequeues items, processes them, and marks them done.
func (ctrl *ArqController) worker(queue workqueue.RateLimitingInterface) func() {
	workFunc := func(ctx context.Context) bool {
		key, quit := queue.Get()
		if quit {
			return true
		}
		defer queue.Done(key)

		logger := klog.FromContext(ctx)
		logger = klog.LoggerWithValues(logger, "queueKey", key)
		ctx = klog.NewContext(ctx, logger)
		err := ctrl.syncHandler(key.(string))
		if err == nil {
			queue.Forget(key)
			return false
		} else {
			log.Log.Infof("ERROR: %v", err)
		}

		utilruntime.HandleError(err)
		queue.AddRateLimited(key)

		return false
	}

	return func() {
		for {
			if quit := workFunc(context.Background()); quit {
				klog.FromContext(context.Background()).Info("resource quota controller worker shutting down")
				return
			}
		}
	}
}

func (ctrl *ArqController) addQuota(logger klog.Logger, obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		logger.Error(err, "Couldn't get key", "object", obj)
		return
	}

	arq := obj.(*v1alpha12.ApplicationsResourceQuota)

	// if we declared an intent that is not yet captured in status (prioritize it)
	if !apiequality.Semantic.DeepEqual(arq.Spec.Hard, arq.Status.Hard) {
		ctrl.missingUsageQueue.Add(key)
		return
	}

	// if we declared a constraint that has no usage (which this controller can calculate, prioritize it)
	for constraint := range arq.Status.Hard {
		if _, usageFound := arq.Status.Used[constraint]; !usageFound {
			matchedResources := []v1.ResourceName{constraint}
			for _, evaluator := range ctrl.evalRegistry.List() {
				if intersection := evaluator.MatchingResources(matchedResources); len(intersection) > 0 {
					ctrl.missingUsageQueue.Add(key)
					return
				}
			}
		}
	}

	// no special priority, go in normal recalc queue
	ctrl.arqQueue.Add(key)
}

// obj could be an *v1.ResourceQuota, or a DeletionFinalStateUnknown marker item.
func (ctrl *ArqController) enqueueArq(logger klog.Logger, obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		logger.Error(err, "Couldn't get key", "object", obj)
		return
	}
	ctrl.arqQueue.Add(key)
}

// syncResourceQuotaFromKey syncs a quota key
func (ctrl *ArqController) syncResourceQuotaFromKey(key string) (err error) {
	startTime := time.Now()

	logger := klog.FromContext(context.Background())
	logger = klog.LoggerWithValues(logger, "key", key)

	defer func() {
		logger.V(4).Info("Finished syncing resource quota", "key", key, "duration", time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	arqObj, exist, err := ctrl.arqInformer.GetIndexer().GetByKey(namespace + "/" + name)
	if !exist {
		logger.Info("Resource quota has been deleted", "key", key)
		return nil
	}
	if err != nil {
		logger.Error(err, "Unable to retrieve resource quota from store", "key", key)
		return err
	}
	arq := arqObj.(*v1alpha12.ApplicationsResourceQuota).DeepCopy()
	return ctrl.syncResourceQuota(arq)
}

// syncResourceQuota runs a complete sync of resource quota status across all known kinds
func (ctrl *ArqController) syncResourceQuota(arq *v1alpha12.ApplicationsResourceQuota) (err error) {
	// quota is dirty if any part of spec hard limits differs from the status hard limits
	statusLimitsDirty := !apiequality.Semantic.DeepEqual(arq.Spec.Hard, arq.Status.Hard)

	// dirty tracks if the usage status differs from the previous sync,
	// if so, we send a new usage with latest status
	// if this is our first sync, it will be dirty by default, since we need track usage
	dirty := statusLimitsDirty || arq.Status.Hard == nil || arq.Status.Used == nil

	used := v1.ResourceList{}
	if arq.Status.Used != nil {
		used = quota.Add(v1.ResourceList{}, arq.Status.Used)
	}
	hardLimits := quota.Add(v1.ResourceList{}, arq.Spec.Hard)

	var errs []error
	newUsage, err := quota.CalculateUsage(arq.Namespace, arq.Spec.Scopes, hardLimits, ctrl.evalRegistry, arq.Spec.ScopeSelector)
	if err != nil {
		// if err is non-nil, remember it to return, but continue updating status with any resources in newUsage
		errs = append(errs, err)
	}

	var rq *v1.ResourceQuota
	rqObj, exists, err := ctrl.rqInformer.GetIndexer().GetByKey(arq.Namespace + "/" + arq.Name + rq_controller.RQSuffix)
	if err != nil {
		errs = append(errs, err)
	} else if exists {
		rq = rqObj.(*v1.ResourceQuota).DeepCopy()
	}

	if exists && rq.Status.Hard != nil && arq.Status.Hard != nil {
		updateUsageFromResourceQuota(arq, rq, newUsage)
	}

	for key, value := range newUsage {
		used[key] = value
	}

	// ensure set of used values match those that have hard constraints
	hardResources := quota.ResourceNames(hardLimits)
	used = quota.Mask(used, hardResources)

	// Create a usage object that is based on the quota resource version that will handle updates
	// by default, we preserve the past usage observation, and set hard to the current spec
	usage := arq.DeepCopy()
	usage.Status = v1alpha12.ApplicationsResourceQuotaStatus{}
	usage.Status.Hard = hardLimits
	usage.Status.Used = used

	dirty = dirty || !quota.Equals(usage.Status.Used, arq.Status.Used)

	// there was a change observed by this controller that requires we update quota
	if dirty {
		_, err = ctrl.aaqCli.ApplicationsResourceQuotas(usage.Namespace).UpdateStatus(context.Background(), usage, metav1.UpdateOptions{})
		if err != nil {
			errs = append(errs, err)
		}
	}
	return utilerrors.NewAggregate(errs)
}

func updateUsageFromResourceQuota(arq *v1alpha12.ApplicationsResourceQuota, rq *v1.ResourceQuota, newUsage map[v1.ResourceName]resource.Quantity) {
	nonSchedulableResourcesHard := rq_controller.FilterNonScheduableResources(arq.Status.Hard)
	if quota.Equals(rq.Spec.Hard, nonSchedulableResourcesHard) && rq.Status.Used != nil {
		nonSchedulableResourcesUsage := rq_controller.FilterNonScheduableResources(rq.Status.Used)
		for key, value := range nonSchedulableResourcesUsage {
			newUsage[key] = value
		}
	}
}
