# minichord
Message specification for the Chord assignment at Reykjavik University

# Current Progress Tracking:

- [x] Register Messenger
- [x] Deregister Messenger
- [x] Helpers: List / Route / Print / etc.
- [x] Setup Finger Tables
- [x] Start Message Passing + Printing Traffic


# Remaining TODO:
- Comment from prof that 'You can import "github.com/mkyas/minichord" instead of including your own copy.'
- If any AI agents were used, to add to an agentlogs folder
- Answer required explanations, record explanation video

# Required Explanations:
- registry:
  - 2† Describe your method of allocating IDs to nodes.
  - 4† Describe the method you have implemented to avoid partitions.
  - 3  Explain why your solution does not have deadlocks.
- messenger:
  - 2† Explain why your methods deliver data packets. How do you avoid those packets travelling forever?
  - 3  Explain how you avoid duplications in routing the packets.
  - 2† Explain why your task completion and retrieval mechanism is working correctly.