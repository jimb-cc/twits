# This script contains methods to add, remove, and retrieve rules from your stream.
# It will also connect to the stream endpoint and begin outputting data.
# Run as-is, the script gives you the option to replace existing rules with new ones and begin streaming data

require 'json'
require 'typhoeus'
require 'awesome_print'
require 'mongo'
require 'slop'

# grab options from command line
opts = Slop.parse do |o|
  o.string '--uri', 'connection string'
  o.string '-m', '--metacoll', 'name of the meta collection', default: 'meta'
end

# create a connection to the database
TWDB = Mongo::Client.new(opts[:uri])

# set the logger level for the mongo driver
Mongo::Logger.logger.level = ::Logger::WARN

# Pull the connections in from the MetaColl
connections = TWDB[:meta].find(type: 'connection').first
ap connections

@bearer_token = connections['bearer_token']
@stream_url = connections['stream_url']
@rules_url = connections['rules_url']

# Pull the rules from the MetaColl
ruledoc = TWDB[:meta].find(type: 'rules').first
@sample_rules = ruledoc['rules']

# Add or remove values from the optional parameters below. Full list of parameters can be found in the docs:
# https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/api-reference/get-tweets-search-stream
params = {"expansions": "geo.place_id","tweet.fields": "created_at,geo,id"}

# Get request to rules endpoint. Returns list of of active rules from your stream 
def get_all_rules
  @options = {
    headers: {
      "User-Agent": "v2FilteredStreamRuby",
      "Authorization": "Bearer #{@bearer_token}"}}
  @response = Typhoeus.get(@rules_url, @options)
  raise "An error occurred while retrieving active rules from your stream: #{@response.body}" unless @response.success?
  @body = JSON.parse(@response.body)
end

# Post request to add rules to your stream
def set_rules(rules)
  return if rules.nil?
  @payload = {add: rules}
  @options = {
    headers: {
      "User-Agent": "v2FilteredStreamRuby",
      "Authorization": "Bearer #{@bearer_token}",
      "Content-type": "application/json"},
    body: JSON.dump(@payload)}
  @response = Typhoeus.post(@rules_url, @options)
  raise "An error occurred while adding rules: #{@response.status_message}" unless @response.success?
end

# Post request with a delete body to remove rules from your stream
def delete_all_rules(rules)
  return if rules.nil?
  @ids = rules['data'].map { |rule| rule["id"] }
  @payload = {delete: {ids: @ids}}
  @options = {
    headers: {
      "User-Agent": "v2FilteredStreamRuby",
      "Authorization": "Bearer #{@bearer_token}",
      "Content-type": "application/json"},
    body: JSON.dump(@payload)}
  @response = Typhoeus.post(@rules_url, @options)
  raise "An error occurred while deleting your rules: #{@response.status_message}" unless @response.success?
end



# Connects to the stream and returns data (Tweet payloads) in chunks
def stream_connect(params)
  puts ("-- stream connect")
  @options = {
    timeout: 0,
    method: 'get',
    headers: {
      "User-Agent": "v2FilteredStreamRuby",
      "Authorization": "Bearer #{@bearer_token}"
    },
    params: params
  }
  @request = Typhoeus::Request.new(@stream_url, @options)
  @request.on_body do |chunk|
    if chunk.eql?("\r\n")
     else
        ap JSON.parse(chunk)
        # insert the tweet object into the DB
        id = TWDB[:tweets].insert_one(JSON.parse(chunk).to_h)
        puts"\n----------------------------\n"
        puts"#{Time.now - @t} secs since last event"
        @t = Time.now
     end
  end
  @request.run
end


@t = Time.now
@rules = get_all_rules
#@rules.nil? puts("-- No Rules")
puts ("-- Deleting all rules\n")
delete_all_rules(@rules) 
puts "-- setting new rules\n"
set_rules(@sample_rules)
@rules = get_all_rules
ap @rules

timeout = 0
while true
  stream_connect(params)
  puts "something's up"
  sleep 2 ** timeout
  timeout += 1
end