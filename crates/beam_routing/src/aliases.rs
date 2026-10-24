//! Catalog aliases such as `chat.default` → a model name (Python `routing/aliases.py`).

use std::collections::HashMap;

use riverbase_core::RiverbaseResult;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelAliasMap {
    aliases: HashMap<String, String>,
}

impl ModelAliasMap {
    #[must_use]
    pub fn new(aliases: HashMap<String, String>) -> Self {
        Self { aliases }
    }

    /// The model name for `name`. A dotted name that is not an alias is an
    /// error; any other name is returned unchanged.
    ///
    /// # Errors
    /// `BEM-041` (public `BEM-422`) for an unknown dotted alias.
    pub fn resolve<'a>(&'a self, name: &'a str) -> RiverbaseResult<&'a str> {
        if let Some(target) = self.aliases.get(name) {
            return Ok(target);
        }
        if name.contains('.') {
            return Err(crate::BEM_041.with_data(format!("unknown model alias: {name}")));
        }
        Ok(name)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.aliases.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_like_python() {
        let map = ModelAliasMap::new([("chat.default".to_owned(), "gpt-x".to_owned())].into());
        assert_eq!(map.resolve("chat.default").unwrap(), "gpt-x");
        assert_eq!(map.resolve("gpt-y").unwrap(), "gpt-y");
        assert_eq!(
            map.resolve("chat.unknown").unwrap_err().errcode.as_str(),
            "BEM-041"
        );
    }
}
