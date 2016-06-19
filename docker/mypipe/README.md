## What is this?
In order to run tests or quickly use mypipe, this Docker Compose set-up allows for a spawning of a MySQL, Zookeeper, and Kafka container.

## Usage
In this directory, for a first time run:

    docker-compose up -d

then followed by the following to see the logs:

    docker-compose logs -f # note, based on the version of docker-compose, -f is required

One the containers are up, you can set up permissions via:

    ./grant-mypipe-permissions

And you can log into the running MySQL database as follows:

./mysql-docker

In order to stop the containers:

    docker-compose stop

And in order to delete them:

    docker-compose rm -fv

Happy hacking!

