require "net/http"
require "json"

HOST = "http://localhost:3030"

def sink_event(aggregate_id, aggregate_sequence: 1)
  url = URI.parse("#{HOST}/sink/#{aggregate_id}")
  request = Net::HTTP::Post.new(url.to_s)
  request["Content-Type"] = "application/json"
  request.body = [
    {
      aggregate_sequence: aggregate_sequence,
      aggregate_type: "Foo",
      event_type: "Bar",
      body: {},
      metadata: {}
    }
  ].to_json
  response = Net::HTTP.start(url.host, url.port) { |http| http.request(request) }
  response.code_type == Net::HTTPNoContent || response.error!
end

def after_events(sequence)
  url = URI.parse("#{HOST}/after/#{sequence}")
  request = Net::HTTP::Get.new(url.to_s)
  response = Net::HTTP.start(url.host, url.port) { |http| http.request(request) }
  response.code_type == Net::HTTPOK || response.error!
  JSON.parse(response.body)
end

def aggregate_events(aggregate_id)
  url = URI.parse("#{HOST}/aggregate/#{aggregate_id}")
  request = Net::HTTP::Get.new(url.to_s)
  response = Net::HTTP.start(url.host, url.port) { |http| http.request(request) }
  response.code_type == Net::HTTPOK || response.error!
  JSON.parse(response.body)
end
