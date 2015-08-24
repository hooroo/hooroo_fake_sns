require "sinatra/base"
require 'json_expressions/rspec'

RSpec.describe "Drain messages", :sqs do

  shared_examples_for "draining messages works" do

    it "works for SQS" do
      topic = sns.topics.create("my-topic")
      queue = sqs.queues.create("my-queue")
      topic.subscribe(queue)

      topic.publish("X")

      $fake_sns.drain
      expect(queue.visible_messages).to eq 1
    end

    context "when drain is called twice" do
      it "does not result in 2 messages in the queue" do
        topic = sns.topics.create("my-topic")
        queue = sqs.queues.create("my-queue")
        topic.subscribe(queue)

        topic.publish("X")

        $fake_sns.drain
        $fake_sns.drain
        expect(queue.visible_messages).to eq 1
      end
    end

    it "only publishes to the subscribed queues for a topic" do
      topic = sns.topics.create("my-topic")
      other_topic = sns.topics.create("other-topic")
      queue = sqs.queues.create("my-queue")
      other_queue = sqs.queues.create("other-queue")
      topic.subscribe(queue)
      other_topic.subscribe(other_queue)

      topic.publish("X")

      $fake_sns.drain
      expect(other_queue.visible_messages).to eq 0
    end

    it "works for SQS with a single message" do
      topic = sns.topics.create("my-topic")
      queue = sqs.queues.create("my-queue")
      topic.subscribe(queue)

      message_id = topic.publish("X")
      topic.publish("Y")

      $fake_sns.drain(message_id)
      expect(queue.visible_messages).to eq 1
    end

    it "works for HTTP" do
      requests = []
      _headers = []
      target_app = Class.new(Sinatra::Base) do
        get("/") { 200 } # check if server started
        post("/endpoint") do
          requests << request.body.read
          _headers << request.env
          200
        end
      end

      app_runner = Thread.new do
        target_app.set :port, 5051
        target_app.run!
      end

      topic = sns.topics.create("my-topic")
      subscription = topic.subscribe("http://localhost:5051/endpoint")

      message_id = topic.publish("X")

      wait_for { Faraday.new("http://localhost:5051").get("/").success? rescue false }

      $fake_sns.drain

      app_runner.kill

      expect(requests.size).to eq 1
      expect(requests.first).to match_json_expression(
        "Type"             => "Notification",
        "Message"          => "X",
        "MessageId"        => message_id,
        "Signature"        => "Fake",
        "SignatureVersion" => "1",
        "SigningCertURL"   => "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-f3ecfb7224c7233fe7bb5f59f96de52f.pem",
        "Subject"          => nil,
        "Timestamp"        => anything,
        "TopicArn"         => topic.arn,
        "UnsubscribeURL"   => "",
      )

      expect(_headers.size).to eq 1
      expect(_headers.first).to match_json_expression({
        "HTTP_X_AMZ_SNS_MESSAGE_TYPE"     => "Notification",
        "HTTP_X_AMZ_SNS_MESSAGE_ID"       => message_id,
        "HTTP_X_AMZ_SNS_TOPIC_ARN"        => topic.arn,
        "HTTP_X_AMZ_SNS_SUBSCRIPTION_ARN" => subscription.arn,
      }.ignore_extra_keys!)

    end

    def wait_for(&condition)
      Timeout.timeout 1 do
        until condition.call
          sleep 0.01
        end
      end
    end
  end

  context "when a logger is not set in AWS" do
    it_behaves_like "draining messages works"
  end

  context "when a logger is set in AWS" do
    let(:logger) do
      logger = double('Logger', info: nil)
      allow(logger).to receive(:to_json) do
        #the actual logger blows up when to_json is called on it due to IO closed issues
        raise "Cannot be used as json"
      end
      logger
    end

    before do
      actually_supplied = AWS.config.send(:supplied)
      allow(AWS.config).to receive(:send).and_call_original
      allow(AWS.config).to receive(:send).with(:supplied).and_return(actually_supplied.merge(logger: logger))
    end

    it_behaves_like "draining messages works"
  end

end
