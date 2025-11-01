use super::errors::TrySelectError;

/// Response from a successful select operation.
///
/// Indicates which operation completed and provides the result data.
#[derive(Debug, Clone)]
pub enum SelectResp<T> {
    /// A receive operation completed successfully.
    Recv { idx: usize, item: T },
    /// A send operation completed successfully.
    Send { idx: usize },
}

impl<T> SelectResp<T> {
    /// Returns the index of the operation that completed.
    pub fn idx(&self) -> usize {
        match self {
            SelectResp::Recv { idx, .. } => *idx,
            SelectResp::Send { idx } => *idx,
        }
    }
}

/// Results from a select operation that may return multiple outcomes.
///
/// This struct wraps a collection of results from select operations and provides
/// convenient methods for accessing successful operations, errors, or iterating over all results.
///
/// # Examples
///
/// ## Basic iteration
///
/// ```no_run
/// use crossfire::mpsc;
/// use crossfire::blocking_select::Select;
///
/// let (tx1, rx1) = mpsc::bounded_blocking(10);
/// let (tx2, rx2) = mpsc::bounded_blocking(10);
///
/// let mut select = Select::new(false);
/// select.recv(&rx1);
/// select.recv(&rx2);
///
/// let results = select.any_ready().select();
///
/// // Iterate over all results
/// for result in &results {
///     match result {
///         Ok(resp) => println!("Success at idx {}", resp.idx()),
///         Err(err) => println!("Error at idx {}", err.idx()),
///     }
/// }
/// ```
///
/// ## Working with successes only
///
/// ```no_run
/// # use crossfire::mpsc;
/// # use crossfire::blocking_select::Select;
/// # let (tx1, rx1) = mpsc::bounded_blocking::<i32>(10);
/// # let (tx2, rx2) = mpsc::bounded_blocking::<i32>(10);
/// # let mut select = Select::new(false);
/// # select.recv(&rx1);
/// # select.recv(&rx2);
/// let results = select.any_ready().select();
///
/// // Get just the successes as an iterator
/// for success in results.successes() {
///     println!("Received at idx {}", success.idx());
/// }
///
/// // Or consume and get a Vec of successes
/// let successes = results.into_successes();
/// ```
///
/// ## Splitting results
///
/// ```no_run
/// # use crossfire::mpsc;
/// # use crossfire::blocking_select::Select;
/// # let (tx1, rx1) = mpsc::bounded_blocking::<i32>(10);
/// # let (tx2, rx2) = mpsc::bounded_blocking::<i32>(10);
/// # let mut select = Select::new(false);
/// # select.recv(&rx1);
/// # select.recv(&rx2);
/// let results = select.any_ready().select();
///
/// // Split into separate vectors
/// let (successes, errors) = results.split();
/// println!("Got {} successes and {} errors", successes.len(), errors.len());
/// ```
///
/// ## Checking result status
///
/// ```no_run
/// # use crossfire::mpsc;
/// # use crossfire::blocking_select::Select;
/// # let (tx1, rx1) = mpsc::bounded_blocking::<i32>(10);
/// # let (tx2, rx2) = mpsc::bounded_blocking::<i32>(10);
/// # let mut select = Select::new(false);
/// # select.recv(&rx1);
/// # select.recv(&rx2);
/// let results = select.any_ready().select();
///
/// if results.all_ok() {
///     println!("All operations succeeded!");
/// }
///
/// println!("Success rate: {}/{}", results.success_count(), results.len());
/// ```
#[derive(Debug)]
pub struct SelectResults<T> {
    results: Vec<Result<SelectResp<T>, TrySelectError<T>>>,
}

impl<T> SelectResults<T> {
    /// Create a new SelectResults from a vector of results.
    pub(crate) fn new(results: Vec<Result<SelectResp<T>, TrySelectError<T>>>) -> Self {
        Self { results }
    }

    /// Returns the number of results (both successes and errors).
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Returns true if there are no results.
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Returns an iterator over references to all results.
    pub fn iter(&self) -> impl Iterator<Item = &Result<SelectResp<T>, TrySelectError<T>>> {
        self.results.iter()
    }

    /// Returns an iterator over references to successful results only.
    pub fn successes(&self) -> impl Iterator<Item = &SelectResp<T>> {
        self.results.iter().filter_map(|r| r.as_ref().ok())
    }

    /// Returns an iterator over references to errors only.
    pub fn errors(&self) -> impl Iterator<Item = &TrySelectError<T>> {
        self.results.iter().filter_map(|r| r.as_ref().err())
    }

    /// Returns the number of successful results.
    pub fn success_count(&self) -> usize {
        self.results.iter().filter(|r| r.is_ok()).count()
    }

    /// Returns the number of error results.
    pub fn error_count(&self) -> usize {
        self.results.iter().filter(|r| r.is_err()).count()
    }

    /// Returns true if all results are successful.
    pub fn all_ok(&self) -> bool {
        self.results.iter().all(|r| r.is_ok())
    }

    /// Returns true if all results are errors.
    pub fn all_err(&self) -> bool {
        self.results.iter().all(|r| r.is_err())
    }

    /// Returns a reference to the result at the given index.
    pub fn get(&self, index: usize) -> Option<&Result<SelectResp<T>, TrySelectError<T>>> {
        self.results.get(index)
    }

    /// Returns a reference to the first successful result, if any.
    pub fn first_success(&self) -> Option<&SelectResp<T>> {
        self.results.iter().find_map(|r| r.as_ref().ok())
    }

    /// Returns a reference to the first error, if any.
    pub fn first_error(&self) -> Option<&TrySelectError<T>> {
        self.results.iter().find_map(|r| r.as_ref().err())
    }

    /// Consumes self and returns a vector of all results.
    pub fn into_vec(self) -> Vec<Result<SelectResp<T>, TrySelectError<T>>> {
        self.results
    }

    /// Consumes self and returns vectors of successes and errors separately.
    pub fn split(self) -> (Vec<SelectResp<T>>, Vec<TrySelectError<T>>) {
        let mut successes = Vec::new();
        let mut errors = Vec::new();

        for result in self.results {
            match result {
                Ok(resp) => successes.push(resp),
                Err(err) => errors.push(err),
            }
        }

        (successes, errors)
    }

    /// Consumes self and returns only the successful results.
    pub fn into_successes(self) -> Vec<SelectResp<T>> {
        self.results.into_iter().filter_map(|r| r.ok()).collect()
    }

    /// Consumes self and returns only the errors.
    pub fn into_errors(self) -> Vec<TrySelectError<T>> {
        self.results.into_iter().filter_map(|r| r.err()).collect()
    }
}

impl<T> IntoIterator for SelectResults<T> {
    type Item = Result<SelectResp<T>, TrySelectError<T>>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.results.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a SelectResults<T> {
    type Item = &'a Result<SelectResp<T>, TrySelectError<T>>;
    type IntoIter = std::slice::Iter<'a, Result<SelectResp<T>, TrySelectError<T>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.results.iter()
    }
}

impl<T> From<Vec<Result<SelectResp<T>, TrySelectError<T>>>> for SelectResults<T> {
    fn from(results: Vec<Result<SelectResp<T>, TrySelectError<T>>>) -> Self {
        Self::new(results)
    }
}

impl<T> Default for SelectResults<T> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl<T: Clone> Clone for SelectResults<T> {
    fn clone(&self) -> Self {
        Self { results: self.results.clone() }
    }
}
