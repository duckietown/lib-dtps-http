# Protocol






# Special URLs 


## Get inside a source


    Link: <url/KEY>; type=get


    <source>/a/0      


## Get the events websocket (inline or not)


    Link: <:events>; type=dtps-events

    topic/:events

## Get the metadata of a resource.


    topic/:index


## Get a range of items

    Link: <:range>; type=dtps-range


The user should use the header:

    Range: items=X-Y

The 
