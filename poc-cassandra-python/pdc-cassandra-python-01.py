from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, TokenAwarePolicy, DCAwareRoundRobinPolicy

cluster = Cluster(contact_points=["n01.cl01","n02.cl01"],
                  default_retry_policy=RetryPolicy(),
                  load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')))
session = cluster.connect("brlav35")


# Creer table et inserer

session.execute("""
create table if not exists users (
    lastname  text primary key,
    firstname text,
    email     text,
    age       int,
    city      text,
)
""")

session.execute("insert into users (lastname, firstname, email, age, city) values ('Lavoie', 'Bruno', 'bl@brunol.com', 34, 'Quebec')")

# Recuperer

result = session.execute("select * from users where lastname = 'Lavoie'")[0]
print result.firstname, result.age

# Update

session.execute("update users set age = 36 where lastname = 'Lavoie'")
result = session.execute("select * from users where lastname='Lavoie' ")[0]
print result.firstname, result.age

session.execute("delete from users where lastname = 'Jones'")

result = session.execute("select * from users")
for x in result:
    print x.age

####################################

# Insert one record into the users table

prepared_stmt = session.prepare ( "INSERT INTO users (lastname, age, city, email, firstname) VALUES (?, ?, ?, ?, ?)")
bound_stmt = prepared_stmt.bind(['Jones', 35, 'Austin', 'bob@example.com', 'Bob'])
stmt = session.execute(bound_stmt)

# Use select to get the user we just entered
prepared_stmt = session.prepare ( "SELECT * FROM users WHERE (lastname = ?)")
bound_stmt = prepared_stmt.bind(['Jones'])
stmt = session.execute(bound_stmt)
for x in stmt:
    print x.firstname, x.age

# Update the same user with a new age
prepared_stmt = session.prepare ("UPDATE users SET age = ? WHERE (lastname = ?)")
bound_stmt = prepared_stmt.bind([36,'Jones'])
stmt = session.execute(bound_stmt)

# Delete the user from the users table
prepared_stmt = session.prepare ("DELETE FROM users WHERE (lastname = ?)")
bound_stmt = prepared_stmt.bind(['Jones'])
stmt = session.execute(bound_stmt)

# Show that the user is gone
results = session.execute("SELECT * FROM users")
for x in results: print x.firstname, x.age