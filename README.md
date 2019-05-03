# eventview
Header only c++ library implementing a basic event-sourced entity graph store and query mechanism. The three main components EventWriter, Publisher, and ViewReader. EventWriter accepts writes, forwards to the event log, and optionally invokes the Publisher. The Publisher owns the logic of correctly updating the in-memory storage. ViewReader owns the logic of performing graph queries against the in-memory storage.

Entities are how nodes in the graph are modelled, and are a set of field->value pairs where fields are strings and values are a variant including srings, longs, doubles, and referenes to other Entities.

All Entities have a unique EntityDescriptor containing a type ID and an Entity ID. Type ids should be unique to a user defined schema for an Entity. Entity IDs should be unique to a specific entity, and will be assigned by the EventWriter on new Entity instance creation.

ViewDescriptors are how graph queries are modelled, and consist of an EntityDescriptor for the root node to query on, and a set of paths through the graph allowing Entity field selection as well as traversal across reference fields (both forward and backward) to selec fields of linked refrenced Entities.

Views are how graph query results are modelled, and consist of the EntityDescriptor and paths provided in the query, along with the value found at the end of each path.

The in-memory storage implements an lwww-element-map, which allows Entity write events to be delivered in any order and still converge on the correct state.

The Publisher and ViewReader are safe to use in a multi-threaded environment. They processes all publish and query operations on a single internal thread fed by a lock-free mechanism. The internal thread lives as long as both the Publisher and ViewReader do, and is cleaned up automatically by their desctruction.
