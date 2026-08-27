from app.services.authenticated_session_service import (
    host_is_allowed,
    parse_allowed_hosts,
    session_state_for_url,
)


def test_host_allowlist_permits_exact_host_and_subdomains():
    hosts = parse_allowed_hosts("portal.example.com, example.org")

    assert host_is_allowed("portal.example.com", hosts)
    assert host_is_allowed("reports.portal.example.com", hosts)
    assert not host_is_allowed("portal.example.com.attacker.test", hosts)


def test_session_is_not_used_when_host_is_out_of_scope(tmp_path):
    state = tmp_path / "state.json"
    state.write_text("{}", encoding="utf-8")

    assert session_state_for_url("https://other.example.com", str(state), "portal.example.com") is None


def test_session_is_used_only_for_allowlisted_host_when_file_exists(tmp_path):
    state = tmp_path / "state.json"
    state.write_text("{}", encoding="utf-8")

    assert session_state_for_url("https://portal.example.com/dashboard", str(state), "portal.example.com") == str(state)
