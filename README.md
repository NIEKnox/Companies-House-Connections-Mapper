# Companies-House-Connections-Mapper
Uses Python, Neo4j and the Companies House API to create networks of connected entities.


# Hey, have you heard about Barbara KAHAN? 
Yeah, she's a woman with [thousands of company appointments]([url](https://find-and-update.company-information.service.gov.uk/officers/auRgqZX1stWO-EoEyget_Mle45c/appointments)). 

This is, apparently, totally legal. People will set up companies where the sole purpose is to just resign as the director and pass the company on to the person who's actually going to use it. In theory, something that saves people a bit of faff, but [this articl]([url](https://www.hamhigh.co.uk/news/21352141.crime-reports-disappear-black-hole-criminals-abused-company-formation-firms/))e suggests that some people abuse this, and criminals are a group of people this service caters to really well.

I'd heard about graph databases so I decided to learn [Neo4j]([url](https://neo4j.com/)) to store this data. Data is stored as nodes, and relationships can be defined in a graph. You can use the crawler_workhorse() function to define a depth (to_depth) of how far you'd like to look for connections, and it'll store that info for you in a database--I locally hosted the [Neo4j desktop browser]([url](https://neo4j.com/deployment-center/?desktop-gdb)). 

If you want to use this, you'll also need a Companies House API key (https://developer.company-information.service.gov.uk/get-started/) so that you can query their database. This is pretty easy, and I didn't have to give any personal info or anything to get it. This is, however, **rate limited**; you can only make 600 api requests every 5 minutes, so mapping out large scale projectsd under these restrictions is going to be tough. I left this running for almost a day and was able to create a database of 84,000 nodes--not nearly enough considering the current highest-scoring 'officer' I've found has [over 120,000 appointments]([url](https://find-and-update.company-information.service.gov.uk/officers/8d_bnTiwfxh8JIr3YfuwkmkWkCg/appointments)). Even in the best case, it would take just under 17 hours to map out every single appointment for this one company, and that's _only_ a depth of one.


# Still to-do
1. Right now it only shows the first 35 connected companies for a given officer lol. I do need to fix this; in practice these companies will still show up if they have any other officers with <=35 appointments, but they'll show up in the browser as disconnected.
2. Rate limiting is currently very permissive; it sleeps for 300 seconds when it hits the rate limit, so it's sleeping the maximum time to make sure it isn't rate limited even though running the code also does take a non-zero amount of time. I need to initiate a global time variable it can compare to, then sleep for 5 minutes - the time since last sleep + 1 second (for safety). 
3. I'd like to implement more flexible system for reading in node properties. Right now the hardcoded properties are:
   Officers: etag (the unique identifier, 'ID_' prepended because neo4j ids can't start with numbers), DOB, name, inactive_count, resigned_count
   Companies: company_number (the unique identifier, 'ID_' prepended because neo4j ids can't start with numbers), company_name, company_number (no ID prepended), company_status 
