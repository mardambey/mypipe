package mypipe.avro.schema

object RegisterSchemaApp extends App {
  if (args.length != 3) {
    println("The RegisterSchema App must be called with exactly three arguments: schema_repo_url schema_string_id avro_name")
    println()
    println("schema_repo_url: The URL of the Schema Repo for the environment you wish to affect.")
    println("schema_string_id: A string used to identify this schema and all it's related versions.")
    println("avro_name: The full name of the Avro bean, located in the JVM's classpath.")

    System.exit(1)
  } else {

    val schemaRepoUrl = args(0)
    val schemaStrId = args(1)
    val avroName = args(2)

    println("The RegisterSchema App has been started with the following arguments:")
    println(s"schema_repo_url:  $schemaRepoUrl")
    println(s"schema_string_id: $schemaStrId")
    println(s"avro_name:        $avroName")

    if (AvroSchemaUtils.registerSchema(schemaRepoUrl, schemaStrId, avroName)) {
      System.exit(0)
    } else {
      System.exit(1)
    }
  }
}

