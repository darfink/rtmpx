#[path = "support/api.rs"]
mod api;
use crate::api::{
    amf0::{self, Amf0Document, Amf0Value as A},
    amf3::{self, Amf3Document, Amf3Value as B},
};

#[test]
fn amf0_self_reference_and_sharing_round_trip() {
    // object { self: ref(0) }, ref(0)
    let wire = b"\x03\x00\x04self\x07\x00\x00\x00\x00\x09\x07\x00\x00";
    let doc = amf0::deserialize_document(&mut &wire[..]).unwrap();
    assert_eq!(doc.roots()[0], doc.roots()[1]);
    assert_eq!(doc.objects().len(), 1);
    let A::Reference(id) = doc.roots()[0] else {
        panic!()
    };
    let A::Object(object) = doc.get(id).unwrap() else {
        panic!()
    };
    assert_eq!(object["self"], A::Reference(id));
    assert_eq!(doc.serialize().unwrap(), wire);
    assert!(amf0::deserialize(&mut &wire[..]).is_err());
}
#[test]
fn amf3_self_reference_and_sharing_round_trip() {
    let wire = b"\x0a\x0b\x01\x09self\x0a\x00\x01\x0a\x00";
    let doc = amf3::deserialize_document(&mut &wire[..]).unwrap();
    assert_eq!(doc.roots()[0], doc.roots()[1]);
    assert_eq!(doc.objects().len(), 1);
    let B::Reference(id) = doc.roots()[0] else {
        panic!()
    };
    let B::Object {
        dynamic: Some(members),
        ..
    } = doc.get(id).unwrap()
    else {
        panic!()
    };
    assert_eq!(members[0].1, B::Reference(id));
    assert_eq!(doc.serialize().unwrap(), wire);
    assert!(amf3::deserialize(&mut &wire[..]).is_err());
}
#[test]
fn construct_mutual_cycles_and_reject_unbound_or_scalar_references() {
    let mut doc = Amf3Document::new();
    let a = doc.insert(B::Null);
    let b = doc.insert(B::Array {
        dense: vec![B::Reference(a)],
        associative: vec![],
    });
    *doc.get_mut(a).unwrap() = B::Array {
        dense: vec![B::Reference(b)],
        associative: vec![],
    };
    doc.roots_mut().push(B::Reference(a));
    let bytes = doc.serialize().unwrap();
    let back = amf3::deserialize_document(&mut bytes.as_slice()).unwrap();
    assert_eq!(back.objects().len(), 2);
    assert!(amf3::serialize(&[B::Reference(a)]).is_err());
    *doc.get_mut(a).unwrap() = B::Null;
    assert!(doc.serialize().is_err());
    let mut doc = Amf0Document::new();
    let a = doc.insert(A::StrictArray(vec![]));
    *doc.get_mut(a).unwrap() = A::StrictArray(vec![A::Reference(a)]);
    doc.roots_mut().push(A::Reference(a));
    let bytes = doc.serialize().unwrap();
    assert_eq!(
        amf0::deserialize_document(&mut bytes.as_slice())
            .unwrap()
            .serialize()
            .unwrap(),
        bytes
    );
}
#[test]
fn every_amf3_complex_type_can_be_shared() {
    let values = [
        B::XmlDoc("x".into()),
        B::Date(1.0),
        B::Array {
            dense: vec![B::Null],
            associative: vec![],
        },
        B::dynamic_object(vec![]),
        B::Xml("x".into()),
        B::ByteArray(vec![1, 2, 3]),
        B::VectorInt {
            fixed: true,
            values: vec![1],
        },
        B::VectorUint {
            fixed: false,
            values: vec![2],
        },
        B::VectorDouble {
            fixed: true,
            values: vec![3.0],
        },
        B::VectorObject {
            type_name: "*".into(),
            fixed: false,
            values: vec![B::Null],
        },
        B::Dictionary {
            weak_keys: false,
            entries: vec![(B::String("x".into()), B::Null)],
        },
        B::Externalizable {
            class_name: "flex.messaging.io.ArrayCollection".into(),
            value: Box::new(B::Array {
                dense: vec![],
                associative: vec![],
            }),
        },
    ];
    for value in values {
        let mut doc = Amf3Document::new();
        let id = doc.insert(value);
        doc.roots_mut().extend([B::Reference(id), B::Reference(id)]);
        let bytes = doc.serialize().unwrap();
        let back = amf3::deserialize_document(&mut bytes.as_slice()).unwrap();
        assert_eq!(back.roots()[0], back.roots()[1]);
        assert_eq!(back.serialize().unwrap(), bytes);
    }
}
#[test]
fn embedded_amf3_cycles_have_independent_reference_scopes() {
    let cycle = b"\x0a\x0b\x01\x09self\x0a\x00\x01";
    let wire = [b"\x11".as_slice(), cycle, b"\x11", cycle].concat();
    let doc = amf0::deserialize_document(&mut wire.as_slice()).unwrap();
    assert_eq!(doc.serialize().unwrap(), wire);
    let A::AvmPlus(value) = &doc.roots()[0] else {
        panic!()
    };
    let B::Reference(first) = **value else {
        panic!()
    };
    let A::AvmPlus(value) = &doc.roots()[1] else {
        panic!()
    };
    let B::Reference(second) = **value else {
        panic!()
    };
    assert_ne!(first, second);
    assert!(doc.get_amf3(second).is_some());
}
#[test]
fn inline_objects_count_toward_reference_indices_and_output_is_transactional() {
    let mut doc = Amf3Document::new();
    let id = doc.insert(B::ByteArray(vec![9]));
    doc.roots_mut().extend([
        B::Array {
            dense: vec![],
            associative: vec![],
        },
        B::Reference(id),
        B::Reference(id),
    ]);
    let bytes = doc.serialize().unwrap();
    let back = amf3::deserialize_document(&mut bytes.as_slice()).unwrap();
    assert_eq!(back.roots()[1], back.roots()[2]);
    let mut out = vec![1, 2, 3];
    assert!(amf3::serialize_into(&[B::String("ok".into()), B::Reference(id)], &mut out).is_err());
    assert_eq!(out, [1, 2, 3]);
    assert!(amf0::deserialize_document(&mut b"\x07\x00\x00".as_slice()).is_err());
    assert!(amf3::deserialize_document(&mut b"\x0a\x00".as_slice()).is_err());
}
#[test]
fn expansion_budget_counts_repeated_subgraphs_before_allocating_the_tree() {
    let mut doc = Amf3Document::new();
    let mut id = doc.insert(B::ByteArray(vec![0; 1024]));
    for _ in 0..20 {
        id = doc.insert(B::Array {
            dense: vec![B::Reference(id), B::Reference(id)],
            associative: vec![],
        });
    }
    doc.roots_mut().push(B::Reference(id));
    let bytes = doc.serialize().unwrap();
    let graph = amf3::deserialize_document(&mut bytes.as_slice()).unwrap();
    assert_eq!(graph.objects().len(), 21);
    assert!(
        graph
            .to_tree(crate::api::amf::TreeLimits::default())
            .is_err()
    );
    assert!(amf3::deserialize(&mut bytes.as_slice()).is_err());
    let mut doc = Amf0Document::new();
    let id = doc.insert(A::StrictArray(vec![A::Utf8String("value".into())]));
    doc.roots_mut().extend([A::Reference(id), A::Reference(id)]);
    let trees = doc.to_tree(crate::api::amf::TreeLimits::default()).unwrap();
    assert_eq!(trees[0], trees[1]);
    assert!(
        doc.to_tree(crate::api::amf::TreeLimits {
            maximum_nodes: 2,
            ..Default::default()
        })
        .is_err()
    );
}
