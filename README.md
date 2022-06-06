# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> MongoDB components for Java

This module is a part of the [Pip.Services](http://pipservices.org) polyglot microservices toolkit. It provides a set of components to implement MongoDB persistence.

The module contains the following packages:
- **Codecs** - contains all the default BSON codecs.
- **Connect** - Connection component to configure MongoDB connection to database.
- **Persistence** - abstract persistence components to perform basic CRUD operations.

<a name="links"></a> Quick links:

* [MongoDB persistence](https://www.pipservices.org/recipies/mongodb-persistence)
* [Configuration](https://www.pipservices.org/recipies/configuration)
* [API Reference](https://pip-services3-java.github.io/pip-services3-mongodb-java/)
* [Change Log](CHANGELOG.md)
* [Get Help](https://www.pipservices.org/community/help)
* [Contribute](https://www.pipservices.org/community/contribute)

## Use

Go to the pom.xml file in Maven project and add dependencies::
```xml
<dependency>
  <groupId>org.pipservices3</groupId>
  <artifactId>pip-services3-mongodb</artifactId>
  <version>3.0.0</version>
</dependency>
```

## Develop

For development you shall install the following prerequisites:
* Java SE Development Kit 8+
* Eclipse Java Photon or another IDE of your choice
* Docker
* Apache Maven

Build the project:
```bash
mvn install
```

Run automated tests:
```bash
mvn test
```

Generate API documentation:
```bash
./docgen.ps1
```

Before committing changes run dockerized build and test as:
```bash
./build.ps1
./test.ps1
./clear.ps1
```

## Contacts

The initial implementation is done by **Sergey Seroukhov**. Pip.Services team is looking for volunteers to 
take ownership over Java implementation in the project.
