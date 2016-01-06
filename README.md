# LogHub

Loghub is a pipeline log, close to [logstash](https://www.elastic.co/products/logstash "Collect, Enrich & Transport Data"). But it's
written in java for improved stability and performance.

It received events from external sources, process them and send them.

All components are organized in many pipeline that can be interconnect. A pipeline goes from one receiver source
that generate events, send throught processor and forward them to a sender.

Receiver source uses decoders that takes bytes messages and generate a event from that.

Sender source uses decoders that take event and produce bytes message that are then send to the configured destination.

All of these five kind of operator (Receivers, Senders, Processors, Coders and Decoders) are java classes that can be derived for
custom usages.

Internally it uses [ØMQ](http://zeromq.org "Distributed Messaging") to forward events. Using it allows to avoid
any synchronized primitives while still using many threads for each processing step and so use all the cores of 
modern servers.

