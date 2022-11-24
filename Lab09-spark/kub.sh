# migration from docker-compose to kubernetes

kompose convert -f docker-compose.yml 
kubectl get pods
kubectl apply -f app-deployment.yml
kubectl apply -f app-service.yml
kubectl get all
kubectl get pvc
kubectl get svc
kubectl port-forward svc/app 8080:8080
