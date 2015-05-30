# data-persistence

So, our task is to implement a service actor for a votings management API.
    
- A voting is two items competing against each other for the votes of users.
- The voting ends when one of the items reaches max number of votes that was specified on voting's creation.
- It should be possible to retrieve results for all present and past votings.

The API is:
  
    POST /votings - where body is a json with fields: "itemAId":string, "itemBId":string, "maxVotes":int
    POST /votings/<votingid> - where body is a json with fields: "votingId":string, "itemId":string, "userId":string
    GEt  /votings returns json with "winningItemId":string (optional), "votes":int, "finished":boolean

                           ----------------------------
                           |   Voting                 |
      0   User1            |                          |
     /|\  --------         |   --------  --------     |
     / \  | Vote |         |   |Item A|  |Item B|     |
          --------         |   --------  --------     |
             Item A -->    |     V: 4      V: 3       |
                           |                          |
                           ----------------------------
Goals:

##  Path 1 (mandatory):
- creating votings, gathering votes and returning results
- basic error handling (voting or item does not exist, vote for a finished voting, duplicate item in a voting)
- all information that is needed to conform to the API must be preserved between application restarts. (hence akka-persistence)

###  Path 2:
- it is illegal to create two votings with two the same items
- it is illegal for a user to vote more than once in a single voting
- state snapshots are used to allow for faster recovery

###  Path 3 (harder):
- a child actor is spawned to manage the state of each voting that is in progress - with its persistence.
- to handle increased load, the VotingsManager actor needs to be partitioned
- use persistAsync instead of persist and deal with the consequences;)


### Readings
1. https://lostechies.com/gabrielschenker/2015/05/26/event-sourcing-revisited
2. http://codebetter.com/gregyoung/2010/02/20/why-use-event-sourcing/
3. http://krasserm.blogspot.co.uk/2011/11/building-event-sourced-web-application.html
4. https://github.com/ironfish/akka-persistence-mongo-samples

#### Some more advanced:
5. https://skillsmatter.com/skillscasts/6387-introducing-event-sourced-microservices
6. https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
7. http://blog.confluent.io/2015/03/04/turning-the-database-inside-out-with-apache-samza/
8. http://krasserm.github.io/2015/01/13/event-sourcing-at-global-scale/
9. http://blog.confluent.io/2015/01/29/making-sense-of-stream-processing/

