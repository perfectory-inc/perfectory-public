// Fixture for tools/semgrep/rules/error-boundary.yml (`opengrep scan --test`): plain JavaScript.
export class Caught extends React.Component {
  // ruleid: error-boundaries-come-from-suspensive
  componentDidCatch(error) {
    console.error(error);
  }

  // ok: error-boundaries-come-from-suspensive
  render() {
    return null;
  }
}
