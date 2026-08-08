pub trait Sealed {}

// Seals a trait only when the redb 5 API preview is enabled. `cfg` cannot be applied to a
// supertrait bound directly, so the bound itself is swapped: with the feature off it resolves to a
// blanket-implemented marker, leaving the trait implementable outside redb for the rest of 4.x.
#[cfg(feature = "experimental-api-5")]
pub use self::Sealed as SealedInApi5;

#[cfg(not(feature = "experimental-api-5"))]
pub trait SealedInApi5 {}

#[cfg(not(feature = "experimental-api-5"))]
impl<T: ?Sized> SealedInApi5 for T {}
