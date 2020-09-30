name := "SmartTrie"

version := "0.1"

scalaVersion := "2.13.3"

scalacOptions := Seq(
  "-Xfatal-warnings",
  "-unchecked",
  "-deprecation",
  "-feature",
  "-opt-warnings:at-inline-failed",
  "-Xlint:_",
  "-Ywarn-dead-code",
  "-Wunused",
  "-Ymacro-annotations"
)

scalacOptions in assembly ++= Seq(
  "-opt:l:inline",
  "-opt-inline-from:**,!java.**,!javax.**,!jdk.**,!sun.**"
)

assemblyJarName in assembly := "SmartTrie.jar"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
