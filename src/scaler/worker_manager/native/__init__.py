"""
Native worker manager.

One unit is one local worker process, spawned directly. This is the primitive that the nested
worker managers run inside each resource they provision.
"""
