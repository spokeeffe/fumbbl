document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('addForm');
  const btn = document.getElementById('submitBtn');

  if (form && btn) {
    form.addEventListener('submit', (e) => {
      const groupId = document.getElementById('group_id').value.trim();
      const rulesetId = document.getElementById('ruleset_id').value.trim();

      // Client-side validation
      if (!groupId || !rulesetId || parseInt(groupId) <= 0 || parseInt(rulesetId) <= 0) {
        e.preventDefault();
        return;
      }

      btn.classList.add('loading');
      btn.querySelector('.btn-text').textContent = 'Fetching...';
    });
  }

  // Select-all checkbox for tournaments
  const checkAll = document.getElementById('checkAll');
  if (checkAll) {
    checkAll.addEventListener('change', () => {
      document.querySelectorAll('input[name="tournament_ids"]')
        .forEach(cb => cb.checked = checkAll.checked);
    });
  }

  // Tournament form validation (shared by all generate actions)
  const tournamentsForm = document.getElementById('tournamentsForm');
  if (tournamentsForm) {
    let clickedBtn = null;
    tournamentsForm.querySelectorAll('button[type="submit"]').forEach(btn => {
      btn.addEventListener('click', () => { clickedBtn = btn; });
    });
    tournamentsForm.addEventListener('submit', (e) => {
      const checked = tournamentsForm.querySelectorAll('input[name="tournament_ids"]:checked');
      if (checked.length === 0) {
        e.preventDefault();
        return;
      }
      if (clickedBtn) {
        clickedBtn.classList.add('loading');
        clickedBtn.querySelector('.btn-text').textContent = 'Fetching...';
      }
    });
  }

  // Auto-dismiss alerts after 5s
  const alerts = document.querySelectorAll('.alert');
  alerts.forEach(alert => {
    setTimeout(() => {
      alert.style.transition = 'opacity 0.4s';
      alert.style.opacity = '0';
      setTimeout(() => alert.remove(), 400);
    }, 5000);
  });
});
