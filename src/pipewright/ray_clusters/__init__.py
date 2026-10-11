"""Ray on Agent Platform: a cluster per Ray step, inside the normal Pipewright lifecycle (CONTEXT.md, Ray clusters).

settings  what the platform allows (manifest `ray`, per tenant) and what a step asks for (`steps.<name>.ray`)
service   the launcher's side: create and delete a step's cluster, clean up clusters a killed step left behind
"""
