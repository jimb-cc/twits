FROM ruby:3.0.2

RUN apt-get update && apt-get install -y net-tools
RUN gem install mongo 
RUN gem install faker
RUN gem install slop

ADD twits.rb /home/
CMD ruby /home/twits.rb -h $HOSTNAME -p $PLAYERS -c $COLL 