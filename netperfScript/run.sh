#!/bin/sh

CLIENTS=$(kubectl get pods -l app=iperf3-client -o name | cut -d'/' -f2)
SCHEDULER=$(kubectl get pods -l app=custom-scheduler -o name| cut -d'/' -f2)

for POD in ${CLIENTS}; do
    until $(kubectl get pod ${POD} -o jsonpath='{.status.containerStatuses[0].ready}'); do
        echo "Waiting for ${POD} to start..."
        sleep 5
    done
    HOST=$(kubectl get pod ${POD} -o jsonpath='{.status.hostIP}')
    kubectl exec -it ${POD} -- iperf3 -c iperf3-server -Z -J -T "Client on ${HOST}" $@ > ${HOST}.tmp 
    cp ${HOST}.tmp  ${HOST}.json
    kubectl cp ${HOST}.json ${SCHEDULER}:/home
    echo
done
