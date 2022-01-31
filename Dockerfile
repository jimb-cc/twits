FROM ruby:3.0.2

RUN apt-get update && apt-get install -y net-tools
RUN gem install mongo 
RUN gem install slop
RUN gem instal typhoeus
RUN gem install awesome_print

ADD twits.rb /home/
CMD ruby /home/twits.rb --uri $URI  