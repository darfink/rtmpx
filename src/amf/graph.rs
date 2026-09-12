//! Documents own their nodes. References are integer IDs, so cycles do not leak.

/// A document-local node ID. IDs must not be transferred between documents.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ObjectId(pub(crate) usize);
impl ObjectId {
    pub fn index(self) -> usize {
        self.0
    }
}

/// Roots plus an arena of complex values. Adding a node does not copy its body.
///
/// Use `insert`, then `get_mut`, to build cycles. Serializers validate that IDs
/// resolve to referenceable values. Equality compares arena layout, not graph
/// isomorphism. Scalar roots need no arena entry.
#[derive(Clone, Debug, PartialEq)]
pub struct Document<V> {
    roots: Vec<V>,
    objects: Vec<V>,
}
impl<V> Default for Document<V> {
    fn default() -> Self {
        Self::new()
    }
}
impl<V> Document<V> {
    pub fn new() -> Self {
        Self {
            roots: Vec::new(),
            objects: Vec::new(),
        }
    }
    pub fn with_capacity(roots: usize, objects: usize) -> Self {
        Self {
            roots: Vec::with_capacity(roots),
            objects: Vec::with_capacity(objects),
        }
    }
    pub fn into_parts(self) -> (Vec<V>, Vec<V>) {
        (self.roots, self.objects)
    }
    pub fn roots(&self) -> &[V] {
        &self.roots
    }
    pub fn roots_mut(&mut self) -> &mut Vec<V> {
        &mut self.roots
    }
    pub fn objects(&self) -> &[V] {
        &self.objects
    }
    pub fn insert(&mut self, value: V) -> ObjectId {
        let id = ObjectId(self.objects.len());
        self.objects.push(value);
        id
    }
    pub fn get(&self, id: ObjectId) -> Option<&V> {
        self.objects.get(id.0)
    }
    pub fn get_mut(&mut self, id: ObjectId) -> Option<&mut V> {
        self.objects.get_mut(id.0)
    }
    pub(crate) fn from_parts(roots: Vec<V>, objects: Vec<V>) -> Self {
        Self { roots, objects }
    }
}

/// Limits for explicit graph-to-tree expansion. Shared subgraphs count each time
/// they appear in the resulting tree; cycles are always rejected.
#[derive(Clone, Copy, Debug)]
pub struct TreeLimits {
    pub maximum_nodes: usize,
    pub maximum_bytes: usize,
    pub maximum_depth: usize,
}
impl Default for TreeLimits {
    fn default() -> Self {
        Self {
            maximum_nodes: 100_000,
            maximum_bytes: 16 * 1024 * 1024,
            maximum_depth: 64,
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum TreeError {
    #[error("Cyclic object reference {0:?}")]
    Cycle(ObjectId),
    #[error("Invalid object reference {0:?}")]
    InvalidReference(ObjectId),
    #[error("Graph expansion exceeds the configured tree budget")]
    Limit,
}
pub(crate) trait GraphValue: Clone {
    fn reference(&self) -> Option<ObjectId>;
    fn check_embedded(
        &self,
        _objects: &[crate::amf3::Amf3Value],
        _limits: &mut TreeLimits,
        _depth: usize,
    ) -> Result<(), TreeError> {
        Ok(())
    }
    fn install_embedded(&mut self, _objects: &[crate::amf3::Amf3Value]) {}

    fn heap_bytes(&self) -> usize;
    fn children(
        &self,
        visitor: &mut dyn FnMut(&Self) -> Result<(), TreeError>,
    ) -> Result<(), TreeError>;
    fn children_mut(&mut self, visitor: &mut dyn FnMut(&mut Self));
}
pub(crate) fn validate<V: GraphValue>(
    value: &V,
    objects: &[V],
    embedded: &[crate::amf3::Amf3Value],
    path: &mut Vec<ObjectId>,
    limits: &mut TreeLimits,
    depth: usize,
) -> Result<(), TreeError> {
    if depth > limits.maximum_depth {
        return Err(TreeError::Limit);
    }
    if let Some(id) = value.reference() {
        if path.contains(&id) {
            return Err(TreeError::Cycle(id));
        }
        let target = objects.get(id.0).ok_or(TreeError::InvalidReference(id))?;
        if target.reference().is_some() {
            return Err(TreeError::InvalidReference(id));
        }
        path.push(id);
        let result = validate(target, objects, embedded, path, limits, depth);
        path.pop();
        return result;
    }
    limits.maximum_nodes = limits
        .maximum_nodes
        .checked_sub(1)
        .ok_or(TreeError::Limit)?;
    limits.maximum_bytes = limits
        .maximum_bytes
        .checked_sub(value.heap_bytes())
        .ok_or(TreeError::Limit)?;
    value.check_embedded(embedded, limits, depth)?;
    value.children(&mut |v| validate(v, objects, embedded, path, limits, depth + 1))
}
pub(crate) fn install<V: GraphValue>(
    value: &mut V,
    objects: &[V],
    embedded: &[crate::amf3::Amf3Value],
) {
    if let Some(id) = value.reference() {
        *value = objects[id.0].clone();
    }
    value.install_embedded(embedded);
    value.children_mut(&mut |v| install(v, objects, embedded));
}
pub(crate) fn expand<V: GraphValue>(
    roots: &[V],
    objects: &[V],
    limits: TreeLimits,
) -> Result<Vec<V>, TreeError> {
    expand_with_embedded(roots, objects, &[], limits)
}
pub(crate) fn expand_with_embedded<V: GraphValue>(
    roots: &[V],
    objects: &[V],
    embedded: &[crate::amf3::Amf3Value],
    mut limits: TreeLimits,
) -> Result<Vec<V>, TreeError> {
    let mut path = Vec::new();
    for value in roots {
        validate(value, objects, embedded, &mut path, &mut limits, 0)?;
    }
    let mut output = roots.to_vec();
    for value in &mut output {
        install(value, objects, embedded);
    }
    Ok(output)
}
