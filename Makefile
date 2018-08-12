.PHONY: release

help:
	@echo "make reconfigure       reconfigure server using files under _deploy/"
	@echo "make deploy            build & deploy the production server"
	@echo "make logs              follow the logs of the production server"

reconfigure:
	scp _deploy/wondertaskserver.service _deploy/wondertaskserver.install.sh 'h2:~/'
	ssh h2 'bash ~/wondertaskserver.install.sh'

deploy:
	mkdir -p /tmp/blobsyncserver-build-linux
	GOOS=linux GOARCH=amd64 go build -o /tmp/blobsyncserver-build-linux/wondertaskserver
	scp /tmp/blobsyncserver-build-linux/wondertaskserver 'h2:~/'
	ssh h2 'sudo install -m755 -gandreyvit -oandreyvit -p ~/wondertaskserver /usr/local/bin && sudo systemctl restart wondertaskserver'

logs:
	ssh h2 'sudo journalctl -f -u wondertaskserver'
