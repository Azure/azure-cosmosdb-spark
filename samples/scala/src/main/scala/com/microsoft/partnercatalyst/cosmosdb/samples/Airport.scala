package com.microsoft.partnercatalyst.cosmosdb.samples

case class Airport(airportId: String,
                   name: String,
                   city: String,
                   country: String,
                   iata: String /*	3-letter IATA code. Null if not assigned/unknown.*/ ,
                   icao: String /*	4-letter ICAO code.*/ ,
                   latitude: Double,
                   longitude: Double,
                   altitude: Double,
                   timezone: Double,
                   dst: String,
                   tz: String,
                   airportType: String,
                   source: String)
