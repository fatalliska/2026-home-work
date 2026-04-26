math.randomseed(os.time())

function init(thread)
end

request = function()
   local method_rand = math.random(1, 3)
   local method
   local body = nil
   local headers = {}

   if method_rand == 1 then
       method = "GET"
   elseif method_rand == 2 then
       method = "PUT"
   else
       method = "DELETE"
   end

   local key = "key_" .. math.random(1, 1000000)

   if method == "PUT" then
       body = string.rep("x", 1024)   -- 1 КБ данных
       headers["Content-Type"] = "application/octet-stream"
   end

   local uri = "/v0/entity?id=" .. key
   return wrk.format(method, uri, headers, body)
end