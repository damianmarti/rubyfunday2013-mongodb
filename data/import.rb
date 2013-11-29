MONGODB_SERVER = ENV['MONGODB_SERVER'] || "localhost"
MONGODB_PORT = ENV['MONGODB_PORT'] || 27017
MONGODB_DB = "movies"

%w{rubygems rdf rdf/ntriples mongo}.each{|r| require r}

include Mongo

def fixed_encoding(data)
	data.object.to_s.unpack('U*').pack('C*').force_encoding("UTF-8")
end


db = MongoClient.new(MONGODB_SERVER, MONGODB_PORT).db(MONGODB_DB)


count = 0
coll = db["actors"]

RDF::Reader.open("data/actor_names.nt") do |reader|
	reader.each_statement do |statement|
		#puts "Subject: #{statement.subject} - Predicate: #{statement.predicate} - Object: #{statement.object}"
		doc = {"uri" => statement.subject.to_s, "name" => fixed_encoding(statement.object)}
		id = coll.insert(doc)
		puts "#{count} actors loaded" if (count += 1) % 100 == 0
	end
end

puts "done actors!"

coll.create_index("uri")
coll.create_index(:name => Mongo::TEXT)

puts "movie names indexes created!"


count = 0
coll = db["movies"]

RDF::Reader.open("data/movie_titles.nt") do |reader|
	reader.each_statement do |statement|
		#puts "Subject: #{statement.subject} - Predicate: #{statement.predicate} - Object: #{statement.object}"
		doc = {"uri" => statement.subject.to_s, "name" => fixed_encoding(statement.object)}
		id = coll.insert(doc)
		puts "#{count} movies loaded" if (count += 1) % 100 == 0
	end
end

puts "done movies!"

coll.create_index("uri")
coll.create_index(:name => Mongo::TEXT)

puts "movie names indexes created!"


count = 0

RDF::Reader.open("data/actor_movies.nt") do |reader|
	reader.each_statement do |statement|
		if statement.predicate == "http://data.linkedmdb.org/resource/movie/actor"
			#puts "Subject: #{statement.subject} - Predicate: #{statement.predicate} - Object: #{statement.object}"
			movie = db["movies"].find_one("uri" => statement.subject.to_s)
			actor = db["actors"].find_one("uri" => statement.object.to_s)
			db["movies"].update(
				{"_id" => movie["_id"]}, 
				{"$push" => 
					{"actors" => 
						{"uri" => actor["uri"], "name" => actor["name"]}
					}
				})
			db["actors"].update(
				{"_id" => actor["_id"]}, 
				{"$push" => 
					{"movies" => 
						{"uri" => movie["uri"], "name" => movie["name"]}
					}
				})			
			puts "#{count} relationships loaded" if (count += 1) % 100 == 0
		end
	end
end

puts "done!"








