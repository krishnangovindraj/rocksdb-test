# Notes

Directed graph database with binary and typed ternary edges.

Supported concepts:
- [ ] `isa`: Node is a subset of other node (transitive)
- [ ] `sub`: Node is a schema subset of another node (transitive) 
- [ ] `plays`: Node can relate another node via a type (inherited)
- [ ] `has`: Node can have an attribute via a type (inherited)
- [ ] `playsa`: Node relates a specific node via a type
- [ ] `hasa`: Node has a specific attribute of a type
- [ ] `attr`: Has a specific value via a type

## Desired data structure

`<byte:node_type><varlong:node_id><byte:edge_type>[<svarlong:connected_offset>...]:[<value>]`

`isa`:`node<bob>isa<person>` `node<person>isa^<bob>`
`plays`:`node<bob>plays<builder>` `node<builder>plays^<bob>`
`relates`:`node<house>relates<builder>` `node<builder>relates^<house>`
`has`:`node<bob>has<name>` `node<name>has^<bob>`
`playsa`:`node<bob>playsa<builder><house>` `node<house>playsa^<builder><bob>`
`hasa`:`node<bob>hasa<name><name:bob>`
`attr`:`attr<name><name:bob>:"bob"`


`ent<entity_type><entity>rel<relation_type><role_type><relation>`
`sub<entity_type><entity_type>`