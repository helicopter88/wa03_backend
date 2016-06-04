require 'em-websocket'
class Chat
  def initialize
    @channels = Hash.new
  end
  def new(ws, cname)
    @channels[cname] = EM::Channel.new
    subscribe(ws, cname)
  end

  def subscribe(ws, cname)
    sid = @channels[cname].subscribe {|m| ws.send(m) }
    @channels[cname].push ("#{sid} joined the chat")
  end

  def send_msg(name, user, msg)
    @channels[name].push {"#{user}: #{msg}"}
  end

EventMachine.run {
  EventMachine::WebSocket.start(:host => "0.0.0.0", :port => 8081) do |ws|
    ws.onopen { |handshake| puts "#{handshake} connected"  }
    ws.onmessage do |msg|
      tokens = msg.split
      case tokens.shift
        when "new"
         new(ws, tokens.shift)
        when "subscribe"
          subscribe(ws, tokens.shift)
        when "send_msg"
          send_msg(tokens.shift, tokens.shift, tokens.shift)
        else
          ws.send "Invalid message"
      end
      end
    end

  }
end