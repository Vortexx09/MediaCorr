# MediaCorr

#1 INICIAR MINIKUBE
minikube start


#2 VERIFICAR NODOS
kubectl get nodes


#3 CREAR IMÁGENES DE DOCKER
#IMAGEN PARA API (DESDE /MEDIACORR)
eval $(minikube docker-env)

docker build -t mediacorr-api:latest -f backend/docker/api/Dockerfile .

#IMAGEN PARA JOBS (DESDE /MEDIACORR/BACKEND)
eval $(minikube docker-env)

docker build -t mediacorr-icolcap:latest -f docker/icolcap/Dockerfile .
 
docker build -t mediacorr-sources:latest -f docker/sources/Dockerfile .

docker build -t mediacorr-ingestor:latest -f docker/ingestor/Dockerfile .

docker build -t mediacorr-filter:latest -f docker/filter/Dockerfile .

docker build -t mediacorr-classifier:latest -f docker/classifier/Dockerfile .

docker build -t mediacorr-correlator:latest -f docker/correlator/Dockerfile .


#4 CREAR NAMESPACE
kubectl apply -f k8s/00-namespace.yaml
kubectl get ns


#5 CREAR ALMACENAMIENTO PV + PVC (DEBE DECIR BOUND)
kubectl apply -f k8s/01-storage/pv.yaml
kubectl apply -f k8s/01-storage/pvc.yaml


#6 CREAR CONFIGMAP
kubectl apply -f k8s/03-config/configmap.yaml
kubectl get configmap -n mediacorr


#7 EJECUTAR PODS DE API, DE RBCA Y DE INSPECCIÓN
#POD DE INSPECCIÓN:
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: data-inspector
  namespace: mediacorr
spec:
  containers:
  - name: inspector
    image: busybox
    command: ["sleep", "3600"]
    volumeMounts:
    - name: data-volume
      mountPath: /data
  volumes:
  - name: data-volume
    persistentVolumeClaim:
      claimName: mediacorr-pvc
EOF
>>


#PODS DE RBCA
kubectl apply -f k8s/06-rbac/api-serviceaccount.yaml
kubectl apply -f k8s/06-rbac/api-role.yaml
kubectl apply -f k8s/06-rbac/api-rolebinding.yaml

#PODS DE API:
kubectl apply -f k8s/05-api/api-deployment.yaml
kubectl apply -f k8s/05-api/api-service.yaml


#8 OBTENER IP DE LA API
minikube service mediacorr-api -n mediacorr


#9 VERIFICAR PODS Y JOBS
kubectl get jobs -n mediacorr
kubectl get pods -n mediacorr


#10 REVISAR LOGS DE LOS PODS 
kubectl logs deployment/mediacorr-api -n mediacorr
kubectl logs job/icolcap-job -n mediacorr
kubectl logs job/sources-job -n mediacorr
kubectl logs job/ingestor-job -n mediacorr
kubectl logs job/filter-job -n mediacorr
kubectl logs job/classifier-job -n mediacorr
kubectl logs job/correlator-job -n mediacorr


#11 VERIFICAR DATOS 
kubectl exec -it data-inspector -n mediacorr -- sh


#12 LIMPIAR TODO:
#REINICIAR API
kubectl rollout restart deployment/mediacorr-api -n mediacorr

#LIMPIAR PODS Y JOBS
kubectl delete jobs --all -n mediacorr
kubectl delete pods --all -n mediacorr

#LIMPIAR IMAGENES DANGLING
eval $(minikube docker-env)
docker image prune -a

