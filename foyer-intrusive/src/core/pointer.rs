//  Copyright 2024 Foyer Project Authors.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//  Copyright 2020 Amari Robinson
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::{fmt::Debug, pin::Pin, ptr::NonNull, rc::Rc, sync::Arc};

/// # Safety
///
/// Pointer operations MUST be valid.
pub unsafe trait Pointer {
    type Item: ?Sized;

    /// # Safety
    ///
    /// Pointer operations MUST be valid.
    unsafe fn from_ptr(item: *const Self::Item) -> Self;

    fn into_ptr(self) -> *const Self::Item;

    fn as_ptr(&self) -> *const Self::Item;
}

/// # Safety
///
/// Pointer operations MUST be valid.
pub unsafe trait DowngradablePointerOps: Pointer {
    type WeakPointer;

    fn downgrade(&self) -> Self::WeakPointer;
}

unsafe impl<'a, T: ?Sized + Debug> Pointer for &'a T {
    type Item = T;

    #[inline]
    unsafe fn from_ptr(raw: *const T) -> &'a T {
        &*raw
    }

    #[inline]
    fn into_ptr(self) -> *const T {
        self
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        *self as *const _
    }
}

unsafe impl<'a, T: ?Sized + Debug> Pointer for Pin<&'a T> {
    type Item = T;

    #[inline]
    unsafe fn from_ptr(raw: *const T) -> Pin<&'a T> {
        Pin::new_unchecked(&*raw)
    }

    #[inline]
    fn into_ptr(self) -> *const T {
        unsafe { Pin::into_inner_unchecked(self) as *const T }
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        self.get_ref() as *const _
    }
}

unsafe impl<T: ?Sized + Debug> Pointer for NonNull<T> {
    type Item = T;

    unsafe fn from_ptr(raw: *const T) -> NonNull<T> {
        NonNull::new_unchecked(raw as *mut _)
    }

    fn into_ptr(self) -> *const T {
        self.as_ptr()
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        NonNull::as_ptr(*self)
    }
}

unsafe impl<T: ?Sized + Debug> Pointer for Box<T> {
    type Item = T;

    #[inline]
    unsafe fn from_ptr(raw: *const T) -> Box<T> {
        Box::from_raw(raw as *mut T)
    }

    #[inline]
    fn into_ptr(self) -> *const T {
        Box::into_raw(self) as *const T
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        self.as_ref() as *const _
    }
}

unsafe impl<T: ?Sized + Debug> Pointer for Pin<Box<T>> {
    type Item = T;

    #[inline]
    unsafe fn from_ptr(raw: *const T) -> Pin<Box<T>> {
        Pin::new_unchecked(Box::from_raw(raw as *mut T))
    }

    #[inline]
    fn into_ptr(self) -> *const T {
        Box::into_raw(unsafe { Pin::into_inner_unchecked(self) }) as *const T
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        self.as_ref().get_ref() as *const _
    }
}

unsafe impl<T: ?Sized + Debug> Pointer for Rc<T> {
    type Item = T;

    #[inline]
    unsafe fn from_ptr(raw: *const T) -> Rc<T> {
        Rc::from_raw(raw)
    }

    #[inline]
    fn into_ptr(self) -> *const T {
        Rc::into_raw(self)
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        Rc::as_ptr(self)
    }
}

unsafe impl<T: ?Sized + Debug> Pointer for Pin<Rc<T>> {
    type Item = T;

    #[inline]
    unsafe fn from_ptr(raw: *const T) -> Pin<Rc<T>> {
        Pin::new_unchecked(Rc::from_raw(raw))
    }

    #[inline]
    fn into_ptr(self) -> *const T {
        Rc::into_raw(unsafe { Pin::into_inner_unchecked(self) })
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        self.as_ref().get_ref() as *const _
    }
}

unsafe impl<T: ?Sized + Debug> Pointer for Arc<T> {
    type Item = T;

    #[inline]
    unsafe fn from_ptr(raw: *const T) -> Arc<T> {
        Arc::from_raw(raw)
    }

    #[inline]
    fn into_ptr(self) -> *const T {
        Arc::into_raw(self)
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        Arc::as_ptr(self)
    }
}

unsafe impl<T: ?Sized + Debug> Pointer for Pin<Arc<T>> {
    type Item = T;

    #[inline]
    unsafe fn from_ptr(raw: *const T) -> Pin<Arc<T>> {
        Pin::new_unchecked(Arc::from_raw(raw))
    }

    #[inline]
    fn into_ptr(self) -> *const T {
        Arc::into_raw(unsafe { Pin::into_inner_unchecked(self) })
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        self.as_ref().get_ref() as *const _
    }
}

unsafe impl<T: ?Sized + Debug> DowngradablePointerOps for Rc<T> {
    type WeakPointer = std::rc::Weak<T>;

    fn downgrade(&self) -> Self::WeakPointer {
        Rc::downgrade(self)
    }
}

unsafe impl<T: ?Sized + Debug> DowngradablePointerOps for Arc<T> {
    type WeakPointer = std::sync::Weak<T>;

    fn downgrade(&self) -> Self::WeakPointer {
        Arc::downgrade(self)
    }
}

#[cfg(test)]
mod tests {
    use std::{boxed::Box, fmt::Debug, mem, pin::Pin, rc::Rc, sync::Arc};

    use super::*;

    /// Clones a `Pointer` from a `*const Pointer::Value`
    ///
    /// This method is only safe to call if the raw pointer is known to be
    /// managed by the provided `Pointer` type.
    #[inline]
    unsafe fn clone_pointer_from_raw<T: Pointer + Clone>(ptr: *const T::Item) -> T {
        use std::{mem::ManuallyDrop, ops::Deref};

        /// Guard which converts an pointer back into its raw version
        /// when it gets dropped. This makes sure we also perform a full
        /// `from_raw` and `into_raw` round trip - even in the case of panics.
        struct PointerGuard<T: Pointer> {
            pointer: ManuallyDrop<T>,
        }

        impl<T: Pointer> Drop for PointerGuard<T> {
            #[inline]
            fn drop(&mut self) {
                // Prevent shared pointers from being released by converting them
                // back into the raw pointers
                // SAFETY: `pointer` is never dropped. `ManuallyDrop::take` is not stable until 1.42.0.
                let _ = T::into_ptr(unsafe { core::ptr::read(&*self.pointer) });
            }
        }

        let holder = PointerGuard {
            pointer: ManuallyDrop::new(T::from_ptr(ptr)),
        };
        holder.pointer.deref().clone()
    }

    #[test]
    fn test_box() {
        unsafe {
            let p = Box::new(1);
            let a: *const i32 = &*p;
            let r = p.into_ptr();
            assert_eq!(a, r);
            let p2: Box<i32> = <Box<i32> as Pointer>::from_ptr(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_rc() {
        unsafe {
            let p = Rc::new(1);
            let a: *const i32 = &*p;
            let r = p.into_ptr();
            assert_eq!(a, r);
            let p2: Rc<i32> = <Rc<_> as Pointer>::from_ptr(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_arc() {
        unsafe {
            let p = Arc::new(1);
            let a: *const i32 = &*p;
            let r = p.into_ptr();
            assert_eq!(a, r);
            let p2: Arc<i32> = <Arc<_> as Pointer>::from_ptr(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_box_unsized() {
        unsafe {
            let p = Box::new(1) as Box<dyn Debug>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = p.into_ptr();
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Box<dyn Debug> = <Box<_> as Pointer>::from_ptr(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn test_rc_unsized() {
        unsafe {
            let p = Rc::new(1) as Rc<dyn Debug>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = p.into_ptr();
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Rc<dyn Debug> = <Rc<_> as Pointer>::from_ptr(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn test_arc_unsized() {
        unsafe {
            let p = Arc::new(1) as Arc<dyn Debug>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = p.into_ptr();
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Arc<dyn Debug> = <Arc<_> as Pointer>::from_ptr(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn clone_arc_from_raw() {
        unsafe {
            let p = Arc::new(1);
            let raw = Arc::as_ptr(&p);
            let p2: Arc<i32> = clone_pointer_from_raw(raw);
            assert_eq!(2, Arc::strong_count(&p2));
        }
    }

    #[test]
    fn clone_rc_from_raw() {
        unsafe {
            let p = Rc::new(1);
            let raw = Rc::as_ptr(&p);
            let p2: Rc<i32> = clone_pointer_from_raw(raw);
            assert_eq!(2, Rc::strong_count(&p2));
        }
    }

    #[test]
    fn test_pin_box() {
        unsafe {
            let p = Pin::new(Box::new(1));
            let a: *const i32 = &*p;
            let r = p.into_ptr();
            assert_eq!(a, r);
            let p2: Pin<Box<i32>> = <Pin<Box<_>> as Pointer>::from_ptr(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_pin_rc() {
        unsafe {
            let p = Pin::new(Rc::new(1));
            let a: *const i32 = &*p;
            let r = p.into_ptr();
            assert_eq!(a, r);
            let p2: Pin<Rc<i32>> = <Pin<Rc<_>> as Pointer>::from_ptr(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_pin_arc() {
        unsafe {
            let p = Pin::new(Arc::new(1));
            let a: *const i32 = &*p;
            let r = p.into_ptr();
            assert_eq!(a, r);
            let p2: Pin<Arc<i32>> = <Pin<Arc<_>> as Pointer>::from_ptr(r);
            let a2: *const i32 = &*p2;
            assert_eq!(a, a2);
        }
    }

    #[test]
    fn test_pin_box_unsized() {
        unsafe {
            let p = Pin::new(Box::new(1)) as Pin<Box<dyn Debug>>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = p.into_ptr();
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Pin<Box<dyn Debug>> = <Pin<Box<_>> as Pointer>::from_ptr(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn test_pin_rc_unsized() {
        unsafe {
            let p = Pin::new(Rc::new(1)) as Pin<Rc<dyn Debug>>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = p.into_ptr();
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Pin<Rc<dyn Debug>> = <Pin<Rc<_>> as Pointer>::from_ptr(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn test_pin_arc_unsized() {
        unsafe {
            let p = Pin::new(Arc::new(1)) as Pin<Arc<dyn Debug>>;
            let a: *const dyn Debug = &*p;
            let b: (usize, usize) = mem::transmute(a);
            let r = p.into_ptr();
            assert_eq!(a, r);
            assert_eq!(b, mem::transmute(r));
            let p2: Pin<Arc<dyn Debug>> = <Pin<Arc<_>> as Pointer>::from_ptr(r);
            let a2: *const dyn Debug = &*p2;
            assert_eq!(a, a2);
            assert_eq!(b, mem::transmute(a2));
        }
    }

    #[test]
    fn clone_pin_arc_from_raw() {
        unsafe {
            let p = Pin::new(Arc::new(1));
            let raw = p.into_ptr();
            let p2: Pin<Arc<i32>> = clone_pointer_from_raw(raw);
            let _p = <Pin<Arc<_>> as Pointer>::from_ptr(raw);
            assert_eq!(2, Arc::strong_count(&Pin::into_inner(p2)));
        }
    }

    #[test]
    fn clone_pin_rc_from_raw() {
        unsafe {
            let p = Pin::new(Rc::new(1));
            let raw = p.into_ptr();
            let p2: Pin<Rc<i32>> = clone_pointer_from_raw(raw);
            let _p = <Pin<Rc<_>> as Pointer>::from_ptr(raw);
            assert_eq!(2, Rc::strong_count(&Pin::into_inner(p2)));
        }
    }
}
