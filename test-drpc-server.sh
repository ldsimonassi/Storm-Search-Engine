curl localhost:8080/buick &
curl localhost:8080/gps &
curl localhost:8080/ipod &
curl localhost:8081/
curl localhost:8081/
curl localhost:8081/
curl -X POST --data "Very nice buick USD1000" localhost:8082/?id=0
curl -X POST --data "Very precise GPS, USD100" localhost:8082/?id=1
curl -X POST --data "Quite useful ipod, USD150" localhost:8082/?id=2
curl localhost:8081/ &
sleep 1
curl localhost:8080/nokia &
sleep 1
curl -X POST --data "Windows mobile nokia.., USD150" localhost:8082/?id=3
curl localhost:8080/led &
curl localhost:8080/tv &
curl localhost:8080/dvd &
echo ""
sleep 1
curl localhost:8081/?max=20
curl -X POST --data "Led TV Full HD" localhost:8082/?id=4
curl -X POST --data "CRT TV" localhost:8082/?id=5
curl -X POST --data "DVD HD UpScaling" localhost:8082/?id=6
