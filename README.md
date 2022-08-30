# Goutube
Streaming utility to build largely-scalable, highly available, fast, secure ***distributed*** streaming APIs!

### **Why build this? What's new here?**

- Simple reason is that distributed systems are cool because of many reasons and some of them are large-scalable, high availability, secure, and fast.
- With this project, I attempt to go in-depth on how to build a system that grows in functionality as well as users and the team developing it.
- It's my attempt to broaden my knowledge and make it strong by developing this real-world end-to-end product.

### **Why choose Go?**

- Simplicity
- Strongly typed and compiled
- Compiles to a single binary with no external dependencies
- Fast and lightweight
- Good coding practices
- Excellent support for network programming and concurrency
- Easy to deploy
  
### Prerequisites
- Kind tool to run a local Kubernetes cluster in Docker. (I am using: kind v0.14.0 go1.18.2 darwin/arm64)
- Go 1.16+


## Goutube Agent Configuration parameters
| Parameters       | Type                                                                                                             | Usage                                                                                                                                                                                                      |
|------------------|------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| BindAddr         | string                                                                                                           | The address that Ring will bind to for communication with other members on the ring. By default this is "0.0.0.0:7946". [Read more](https://www.serf.io/docs/agent/options.html#bind)                      |
| RPCPort          | int                                                                                                              | The address that Ring will bind to for the member's RPC server. By default this is "127.0.0.1:7373", allowing only loopback connections. [Read more](https://www.serf.io/docs/agent/options.html#rpc-addr) |
| NodeName         | string                                                                                                           | Unique node name to identify this member.                                                                                                                                                                  |
| SeedAddresses    | []string                                                                                                         | Addresses of other members to join upon start up.                                                                                                                                                          |
| VirtualNodeCount | int                                                                                                              | Number of virtual nodes to create on the ring for this member.                                                                                                                                             |
| HashFunction     | [HashFunction](https://github.com/Brijeshlakkad/ring/blob/f6306cf287105f18f831db916ef01823ef867fd4/types.go#L10) | Hash function to calculate position of the server on the ring.                                                                                                                                             |
| MemberType       | [MemberType](https://github.com/Brijeshlakkad/ring/blob/ff61485ce23d72714bfb67d7201dc42f4933afa1/types.go#L16)   | Type of the membership: 1. ShardMember 2. LoadBalancerMember.                                                                                                                                              |

### **Can I contribute to this project?**
Feel free to create a PR, I’m more than happy to review and merge it.

### **What's the long-term goal?**

- Onboard videos and documentation
- Clean code, full test coverage and minimal tech debt

# Thank you!
Feel free to create an issue, if you have any problem running this distributed system or any suggestions.