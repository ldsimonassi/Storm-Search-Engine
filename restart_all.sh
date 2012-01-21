ssh -i darioarquitectura.pem ubuntu@ec2-107-20-89-124.compute-1.amazonaws.com ./stop_server.sh
ssh -i darioarquitectura.pem ubuntu@ec2-50-16-110-129.compute-1.amazonaws.com ./stop_server.sh
ssh -i darioarquitectura.pem ubuntu@ec2-23-20-23-116.compute-1.amazonaws.com ./stop_server.sh

ssh -i darioarquitectura.pem ubuntu@ec2-107-20-89-124.compute-1.amazonaws.com ./run_server.sh
ssh -i darioarquitectura.pem ubuntu@ec2-50-16-110-129.compute-1.amazonaws.com ./run_server.sh
ssh -i darioarquitectura.pem ubuntu@ec2-23-20-23-116.compute-1.amazonaws.com ./run_server.sh
