'use strict';

// Collapsible examples
let candidates = document.querySelectorAll('p.rubric');
for (let candidate of candidates) {
    if (candidate.innerText == 'Example') {
        let sibling = candidate.nextElementSibling;
        if (sibling.classList) {
            if (sibling.classList.contains('literal-block-wrapper') || sibling.classList.value.includes('highlight-')) {
                //sibling.style.display = 'none';
                candidate.innerHTML = '<a class="example-collapsible">Example<a>';
                candidate.addEventListener('click', function(event) {
                    if (sibling.style.display == 'none') {
                        sibling.style.display = '';
                    } else {
                        sibling.style.display = 'none';
                    }
                });
            }
        }
    }
}
