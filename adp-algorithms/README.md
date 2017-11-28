Advanced Data Profiling Seminar
===============================

## Building

Build requirement is only Java 1.8. The build system is Gradle.
In order to build all algorithm JARs, invoke:

    ./gradlew assemble

You will not be required to install Gradle - upon first invocation an appropriate distribution will
be downloaded and run automatically.  
You will find the algorithm JARs inside the `<algorithm>/build/libs` directory.

## Algorithms

Algorithm implementations (and possibly companion projects) should reside in their own subdirectory.
Do not forget to add those to the list of known submodules inside the [`settings.gradle`](settings.gradle).
