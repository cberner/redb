use crate::Result;
use crate::tree_store::btree_iters::{BtreeRangeIter, RangeLeafEntry, RangeVisit};
use crate::tree_store::subtree_rebuild::{
    InProgressSubtree, LeafRewrite, SealedSubtree, SubtreeBuilder, SubtreeRebuildContext,
    finish_rebuilt_root,
};
use crate::tree_store::{BtreeHeader, PageHint, PageNumber, PageResolver};
use crate::types::{Key, Value};
use std::borrow::Borrow;
use std::ops::RangeBounds;

pub(super) struct Retain {
    builder: SubtreeBuilder,
    in_progress: InProgressSubtree,
    current_leaf: Option<LeafRewrite>,
    removed: u64,
}

impl Retain {
    pub(super) fn new() -> Self {
        Self {
            builder: SubtreeBuilder::left_to_right(),
            in_progress: InProgressSubtree::new(),
            current_leaf: None,
            removed: 0,
        }
    }

    pub(super) fn execute<'r, K, V, KR, F>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        header: BtreeHeader,
        range: &'_ impl RangeBounds<KR>,
        resolver: PageResolver,
        predicate: &mut F,
    ) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
        KR: Borrow<K::SelfType<'r>> + 'r,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        let mut iter = BtreeRangeIter::<K, V>::new_with_subtree_metadata(
            range,
            Some(header),
            resolver,
            PageHint::None,
        )?;
        while let Some(result) =
            iter.next_with_visitor(|event| self.visit(context, event, predicate))
        {
            result?;
        }
        Ok(())
    }

    fn visit<K: Key, V: Value, F>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        event: RangeVisit<'_>,
        predicate: &mut F,
    ) -> Result
    where
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        match event {
            RangeVisit::BranchEnter { branch } => {
                self.in_progress.enter_branch(branch.clone());
                Ok(())
            }
            RangeVisit::SkippedSubtree { subtree } => {
                self.in_progress
                    .push_subtree(SealedSubtree::from_range(subtree.clone()));
                Ok(())
            }
            RangeVisit::LeafEntry { entry } => self.visit_leaf_entry(context, entry, predicate),
            RangeVisit::LeafExit { subtree } => {
                let page_number = subtree.page_number();
                if self.current_leaf_page() == Some(page_number) {
                    self.complete_current_leaf(context)?;
                }
                Ok(())
            }
            RangeVisit::BranchExit { branch } => {
                if let Some(replaced_page) =
                    self.in_progress
                        .exit_branch_into(context, &mut self.builder, branch)?
                {
                    context.conditional_free(replaced_page);
                }
                Ok(())
            }
        }
    }

    pub(super) fn finish<K: Key, V: Value>(
        mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        header: BtreeHeader,
    ) -> Result<Option<BtreeHeader>> {
        self.complete_current_leaf(context)?;
        if self.removed == 0 {
            return Ok(Some(header));
        }

        let replaced_pages = self.in_progress.finish_into(context, &mut self.builder)?;
        for page in replaced_pages {
            context.conditional_free(page);
        }

        finish_rebuilt_root(context, self.builder, header, self.removed)
    }

    fn visit_leaf_entry<K: Key, V: Value, F>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        entry: RangeLeafEntry<'_>,
        predicate: &mut F,
    ) -> Result
    where
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        assert!(
            self.current_leaf_page()
                .is_none_or(|page| page == entry.page_number())
        );
        let new_leaf = self.current_leaf.is_none();
        if new_leaf {
            self.current_leaf = Some(LeafRewrite::new(entry));
        }

        let entry_accessor = entry.entry::<K, V>();
        let key = entry_accessor.key();

        if !predicate(K::from_bytes(key), V::from_bytes(entry_accessor.value())) {
            self.mark_removed::<K, V>(context, entry.entry_index())?;
        }
        Ok(())
    }

    fn mark_removed<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
        entry_index: usize,
    ) -> Result {
        let leaf = self
            .current_leaf
            .as_mut()
            .expect("range visitor must set current leaf before predicate");
        let first_removed_in_leaf = leaf.mark_removed(entry_index);
        if first_removed_in_leaf {
            self.in_progress.mark_changed();
            self.in_progress.flush_into(context, &mut self.builder)?;
        }
        self.removed += 1;
        Ok(())
    }

    fn complete_current_leaf<K: Key, V: Value>(
        &mut self,
        context: &mut SubtreeRebuildContext<'_, K, V>,
    ) -> Result {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.complete_into(context, &mut self.in_progress, &mut self.builder)?;
        }
        Ok(())
    }

    fn current_leaf_page(&self) -> Option<PageNumber> {
        self.current_leaf.as_ref().map(LeafRewrite::page_number)
    }
}
