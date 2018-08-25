.PHONY: help
.PHONY: reconfigure
.PHONY: deploy
.PHONY: logs

# -- configuration -------------------------------------------------------------
server=h2

# ------------------------------------------------------------------------------
help:
	@echo "make initial-setup     deploy & reconfigure"
	@echo "make reconfigure       reconfigure server using files under _deploy/"
	@echo "make deploy            build & deploy the production server"
	@echo "make logs              follow the logs of the production server"


# -- server deployment ----------------------------------------------------------

initial-setup: reconfigure deploy

reconfigure:
	scp _deploy/wondertasks.sh '$(server):~/'
	ssh $(server) 'bash ~/wondertasks.sh --reconfigure'

deploy:
	GOOS=linux GOARCH=amd64 go build -o /tmp/wondertasks-linux-amd64
	scp /tmp/wondertasks-linux-amd64 _deploy/wondertasks.sh '$(server):~/'
	ssh $(server) 'bash ~/wondertasks.sh --deploy'

logs:
	ssh $(server) 'sudo journalctl -f -u wondertasks'
