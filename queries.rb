MONGODB_SERVER = ENV['MONGODB_SERVER'] || "localhost"
MONGODB_PORT = ENV['MONGODB_PORT'] || 27017
MONGODB_DB = "movies"

%w{rubygems rdf rdf/ntriples mongo awesome_print}.each{|r| require r}

include Mongo

db = MongoClient.new(MONGODB_SERVER, MONGODB_PORT).db(MONGODB_DB)


puts "Al Pacino"
al_pacino = db["actors"].find_one({"uri" => "http://data.linkedmdb.org/resource/actor/29726"})
ap al_pacino
puts al_pacino["name"]
al_pacino["movies"].each {|movie| puts movie["name"]}

puts "The Godfather"
godfather = db["movies"].find_one({"uri" => "http://data.linkedmdb.org/resource/film/43338"})
ap godfather
puts godfather["name"]
godfather["actors"].each {|actor| puts actor["name"]}

search = db.command({:text => 'actors', :search => 'Pacino'})
ap search
puts "Cantidad de actores 'Pacino': #{search["results"].size}"
puts search["results"].first["obj"]["name"]

search = db.command({:text => 'movies', :search => 'Love'})
puts "Cantidad de peliculas que contengan la palabra 'Love' (100 max por default): #{search["results"].size}"
search["results"][0..9].each {|s| puts s["obj"]["name"]}

search = db.command({:text => 'movies', :search => 'amor'})
puts "Cantidad de peliculas que contengan la palabra 'Amor' (100 max por default): #{search["results"].size}"
search["results"][0..9].each {|s| puts s["obj"]["name"]}

al_actors = db["actors"].find({"name" => /^Al/}).to_a
puts "Cantidad de actores que empiezan con Al: #{al_actors.size}"

search = db.command({:text => 'actors', :search => 'Damian'})
ap search
puts "Cantidad de actores 'Damian': #{search["results"].size}"
search["results"].each {|actor| puts actor["obj"]["name"]}

damian_actors = db["actors"].find({"name" => /^Damian/}).to_a
puts "Cantidad de actores que empiezan con Damian: #{damian_actors.size}"
damian_actors.each {|actor| puts actor["name"]}
puts "Explain del find con reg exp"
ap db["actors"].find({"name" => /^Damian/}).explain

puts "Al Pacino movies buscando desde movies"
al_pacino_movies = db["movies"].find({ 'actors.uri' => 'http://data.linkedmdb.org/resource/actor/29726'})
al_pacino_movies.each {|movie| puts movie["name"]}

puts "Explain del Al Pacino movies buscando desde movies"
ap db["movies"].find({ 'actors.uri' => 'http://data.linkedmdb.org/resource/actor/29726'}).explain