# LogHub

Loghub is a pipeline log, close to logstah.

It received events from external sources, process them and output them.

All components are organized in many pipeline that can be interconnect. A pipeline goes from one input source
that generate events, send throught transformers and goes to one output.

Input source uses decoders that takes bytes messages and generate a event from that.

Output source uses decoders that take event and produce bytes message that are then send to the configured destination.

All of these five kind of operator (Receivers, Senders, Transformers, Coders and Decoders) are java classes that can be derived for
custom usages.
