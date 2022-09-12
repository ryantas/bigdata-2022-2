1.- mongodb getting started:

1.1 intallation [tutorial](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/)

`sudo servictml start `

```bash
mongo
dbs
show dbs
use MovieLens
db.getCollectionNames()
```

output:

```bash
{ "_id" : 5, "title" : "Father of the Bride Part II (1995)", "genres" : "Comedy" }
{ "_id" : 12, "title" : "Dracula: Dead and Loving It (1995)", "genres" : "Comedy|Horror" }
{ "_id" : 13, "title" : "Balto (1995)", "genres" : "Animation|Children's" }
{ "_id" : 14, "title" : "Nixon (1995)", "genres" : "Drama" }
{ "_id" : 15, "title" : "Cutthroat Island (1995)", "genres" : "Action|Adventure|Romance" }
{ "_id" : 16, "title" : "Casino (1995)", "genres" : "Drama|Thriller" }
{ "_id" : 17, "title" : "Sense and Sensibility (1995)", "genres" : "Drama|Romance" }
{ "_id" : 19, "title" : "Ace Ventura: When Nature Calls (1995)", "genres" : "Comedy" }
{ "_id" : 20, "title" : "Money Train (1995)", "genres" : "Action" }
```

2.- using a docker image

Verify if is in use with `sudo systemctl status mongodb` if is active run  `sudo service mongodb stop`


3.- references

* [Using mongo with python](https://towardsdatascience.com/using-mongodb-with-python-bcb26bf25d5d)
* [operations](https://www.w3schools.com/python/python_mongodb_insert.asp)
* [Mongodb and redis](https://www.youtube.com/watch?v=oltouxKTkFw&ab_channel=ParadigmaDigital)
