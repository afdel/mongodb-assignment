
db = db.getSiblingDB('assignment1')

db.microblog.find( { 'timestamp' : { $type : 2 } } ).forEach( function (x) {
	x.timestamp = new Date(x.timestamp); // convert field to date
	db.microblog.save(x);
 	//print("Working");
	//print(x.timestamp);
});
print("Finish");
