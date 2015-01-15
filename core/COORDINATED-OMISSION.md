YCSB in it's original form suffers from Coordinated Omission[1]:
* Load is controlled by response time
* Measurement does not account for missing time
* Measurement starts at beginning of request rather than at intended beginning
To provide a minimal correction patch I propose:
1. Replace internal histogram implementation with HdrHistogram:
HdrHistogram offers a dynamic range of measurement at a given precision and will
improve the fidelity of reporting. It allows capturing a much wider range of latencies.
We could add controls for corrected/uncorrected measurement. This is appropriate for a
uniform load test, but not for other loads. The mixing of different Ops also makes
this correction unreliable. This could work for a global histogram. 
2. Track intended operation start and report latencies from that point in time:
This will require the introduction of a new measurement point and will inevitably
include measuring some of the internal preparation steps of the load generator.
These overhead should be negligible in the context of a network hop, but could
be corrected for by estimating the load-generator overheads (e.g. by measuring a
no-op DB).

Further correction suggestions:
1. Correction load control: currently after a pause the load generator will do
operations back to back to catchup, this leads to a flat out throughput mode
of testing as opposed to controlled load.
2. Move to async model: Scenarios where Ops have no dependency could delegate the
Op execution to a threadpool and thus separate the request rate control from the
synchronous execution of Ops. Measurement would start on queuing for execution.

1. https://groups.google.com/forum/#!msg/mechanical-sympathy/icNZJejUHfE/BfDekfBEs_sJ