require 'rubygems'
require 'mechanize'

agent = Mechanize.new
agent.get('http://google.com/') do |page|
  res = page.form_with(:name=> 'f') do |form|
    pp form
    form.q = 'ruby mechanize'
  end.submit
  pp res
end