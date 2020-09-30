include("cache-core")
include("cache-spring")
include("cache-spring-webmvc")


pluginManagement {
    resolutionStrategy {
        eachPlugin {
            when (requested.id.id) {
                "org.jetbrains.kotlin.jvm" -> useVersion(Versions.kotlinVersion)
            }
        }
    }
}
