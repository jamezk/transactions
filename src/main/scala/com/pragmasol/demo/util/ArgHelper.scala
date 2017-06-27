/*
 * Copyright 2017 Pragmasol
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pragmasol.demo.util

/**
 * Very simple trait that parses optional arguments from the command line
 * and creates a map of them which can be used in your application.
 * In your application pass arguments like foo=bar and then cmdArgs will contain
 * that argument with that value. Obviously in your application you can easily provide
 * defaults by using Maps' built-in Option capability e.g.
 *
 * val numberOfWorkers = getArgOrDefault("numberOfWorkers","2").toInt
 *
 * This is a very rudimentary implementation and all arguments are treated
 * as strings. There is scope to provide a more complex mechanism but this
 * does the job for now.
 *
 */
trait ArgHelper {

  required : App =>

  //lazily evaluated because args won't be available immediately
  //uses an Option class to ensure things don't break if no arguments
  //are supplied
  private lazy val cmdArgs = parse(Option(args))

  def parse(args : Option[scala.Array[String]]) : Map[String,String] = {
    args.getOrElse(Array[String]())
      .filter(_.split("=").length > 0).map { arg =>
      val argPair = arg.split("=")
      (argPair(0), argPair(1))
    }.toMap
  }

  def getArgOrDefault(key : String, default : => String) = cmdArgs.getOrElse(key,default)

  def getArg(key : String) = getArgOrDefault(key,"")

  def argIsProvided(key : String) = cmdArgs.contains(key)

  def readFromStdIn(msg : String) = {
    Console.readLine(msg)
  }

}
