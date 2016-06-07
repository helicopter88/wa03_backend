require 'rss'
require 'open-uri'
require 'json'
#
# Class to fetch financial related news from yahoo finance
#
class News
  def parse_tokens(tokens)
    action = tokens.shift
    args = tokens
    send action, *args
  end
  
  def get_international_news
    rss_fetch 'https://uk.finance.yahoo.com/news/category-international/?format=rss'
  end

  def get_news(symbol)
    rss_fetch "https://feeds.finance.yahoo.com/rss/2.0/headline?s=#{symbol}&region=US&lang=en-US"
  end

  private

  def rss_fetch(url)
    list = []
    open(url) do |rss|
      feed = RSS::Parser.parse(rss)
      feed.items.each do |item|
        list << { title: item.title, link: item.link }
      end
    end
    list.to_json
  end
end
