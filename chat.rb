require 'em-websocket'

class Chat
  def initialize
    @channels = {}
  end

  def method_missing(_m, *_args)
    puts 'Invalid method called'
  end

  def new_chan(ws, cname, user)
    @channels[cname] = EM::Channel.new
    subscribe(ws, cname, user)
  end

  def subscribe(ws, cname, user)
    @channels[cname].subscribe { |m| ws.send(m) }
    @channels[cname].push("#{user} joined the chat")
  end

  def send_msg(_ws, cname, user, *msg)
    str = ''
    msg.map { |w| str += "#{w} " }
    @channels[cname].push "#{user}: #{str}"
  end
end

@chat = Chat.new

EventMachine.run do
  EventMachine::WebSocket.start(host: '0.0.0.0', port: 8081) do |ws|
    ws.onopen { |handshake| puts "#{handshake} connected" }
    ws.onmessage do |msg|
      tokens = msg.split
      @chat.send(tokens.shift, ws, *tokens)
    end
  end
end
