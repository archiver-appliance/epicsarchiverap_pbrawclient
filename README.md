# EPICS Archiver Appliance java client library

This is a Java client jar for the binary protocol used by the EPICS Archiver Appliance. 
See the archiveviewer for some sample usage. All the action starts in `RawDataRetrieval.getDataForPV`

## Using the library

Released versions are published to Maven Central under `org.epics:pbrawclient`.

Maven:

```xml
<dependency>
    <groupId>org.epics</groupId>
    <artifactId>pbrawclient</artifactId>
    <version>0.2.3</version>
</dependency>
```

Gradle:

```groovy
implementation 'org.epics:pbrawclient:0.2.3'
```

Snapshots of `master` are published to the Central snapshot repository at
`https://central.sonatype.com/repository/maven-snapshots/`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build, test and cut a release.
