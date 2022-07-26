FROM docker.elastic.co/logstash/logstash:8.1.0
RUN bin/logstash-plugin install logstash-integration-kafka
RUN bin/logstash-plugin install logstash-input-http_poller
RUN mkdir myFolder
RUN touch $PWD/myFolder/input.txt
RUN touch $PWD/myFolder/output.txt

