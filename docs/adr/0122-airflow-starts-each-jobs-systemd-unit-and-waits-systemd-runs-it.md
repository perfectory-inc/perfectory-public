# ADR 0122: Airflow 는 예약 작업의 systemd 유닛을 시작시키고 기다릴 뿐, 실행은 지금처럼 systemd 가 한다

- Status: Accepted
- Date: 2026-10-01
- Builds on: [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md)(예약 작업은 Airflow),
  [ADR-0086](./0086-the-pipeline-graph-names-every-dataset-once.md)(데이터 지도),
  [ADR-0116](./0116-dawneer-keeps-staff-tokens-on-its-rust-server.md)(직원 로그인)

## Context

ADR-0118 은 예약 작업을 Airflow 로 옮긴다고 정했지만, Airflow 가 그 작업을 **무엇으로, 어떤 권한으로** 돌리는지는
정하지 않았다. 2026-10-01 ai-server 실측:

- 옮길 타이머 다섯(원천 확인, 이벤트 발행, 지도 편집 접기 둘, 계보 검토)은 모두 같은 모양이다. systemd 유닛이
  `/etc/foundation-platform/*.env`(root 만 읽음)를 읽어 환경으로 넘기고, 사용자 `foundation-platform`(docker 그룹,
  로그인 불가 계정)으로 `scripts/ops/*.sh` 를 돌리며, 제한 시간(`TimeoutStartSec`)을 건다.
- 처음 시험한 방식(Airflow 가 Docker 소켓으로 작업마다 컨테이너를 띄움)은 서버에서 돌았지만 저장소의 컨테이너 정책이
  Docker 소켓 연결을 금지한다. 소켓을 쥔 스케줄러는 뚫리면 서버 root 와 같다.
- 작업이 읽고 쓰는 데이터는 데이터 지도(`pipeline-graph.v1.json`)의 연결(edge)로 이미 이름이 있다.
- 사례: 대규모 Airflow 운영은 스케줄러에 실행 권한을 몰지 않고, 실행 환경(Kubernetes 파드, 원격 워커)이 작업 정의를
  갖고 스케줄러는 시작·관찰만 한다. 원격 실행 권한은 그 작업만 할 수 있는 좁은 자격으로 준다(SSH forced command,
  sudoers 명령 목록).

## Decision

1. **실행은 systemd 가 한다.** 작업 정의(실행 파일·환경 파일·사용자·제한 시간)는 지금의 systemd 서비스 유닛 그대로다.
   Airflow 가 맡는 것은 일정·재시도·실행 이력·알림·계보다.
2. **Airflow 의 권한은 "그 유닛들을 시작·중지하고 그 기록을 읽는 것"뿐이다.** 서버에 로그인할 수 없는 전용 계정
   `foundation-scheduler` 를 두고, Airflow 의 SSH 열쇠는 그 계정에서 명령 하나(`scripts/ops/start-scheduled-job.sh`)만
   실행하게 묶는다(forced command, `restrict`, Docker 망에서만). 그 명령은 작업 목록의 켜진 작업만 받아
   `systemctl start --wait` 로 시작해 끝날 때까지 기다리고, 그 실행분의 기록만 돌려준다. 연결이 끊기면 그 유닛을
   멈춘다. sudo 허용은 작업 목록에서 만든 정확한 유닛 이름의 start·stop 뿐이다.
3. **작업 목록은 한 곳이다.** `platforms/foundation-platform/orchestration/jobs.v1.json` 이 작업마다 systemd 서비스
   이름·일정·자원 묶음·수행하는 데이터 지도 연결(edge id)·켜짐 여부를 적는다. 입력·출력은 지도의 연결 끝점에서 읽어
   OpenLineage 로 보낸다. 지도에 없는 연결이나 저장소에 없는 유닛을 적으면 시험이 막는다.
4. **한 작업은 한 곳에서만 돈다.** 작업을 켜는(`enabled`) 변경이 그 작업의 systemd 타이머를 저장소에서 지우고,
   릴리스가 서버의 타이머를 끄며, Airflow 배포가 DAG 를 켠다. 켜진 작업에 타이머가 남거나 꺼진 작업에 타이머가 없으면
   시험이 막는다.
5. **Spark 를 쓰는 작업은 한 번에 하나다.** Airflow 자원 묶음(pool) `spark` 는 자리가 하나다(메모리 예산의 전제).
6. **Airflow 는 작게 돈다.** LocalExecutor, 전용 메타데이터 DB, API 서버·스케줄러·DAG 처리기. 모든 컨테이너는
   상한을 갖고 예산 가드를 통과한다.
7. **로그인은 Zitadel 이다.** FAB 인증 관리자의 OIDC, 직원 앱 목록에 `airflow`, 스스로 가입 없음(미리 등록한 직원만).
8. DB 백업은 ADR-0118 대로 systemd 타이머로 남는다.

## Consequences

- 실행 방법의 정본이 저장소(유닛 파일 + 작업 목록)로 오고, 스크립트·환경 파일·실행 계정은 바뀌지 않는다.
- 서버에 계정 하나, sudoers 파일 하나, SSH 열쇠 하나가 생긴다. 모두 릴리스 스크립트가 저장소 내용으로 설치하고 다시 맞춘다.
- 작업 기록은 systemd 저널에 그대로 남고, Airflow 화면에서도 같은 내용을 본다.
- 사람이 서버에서 손으로 띄우는 큰 Spark 적재를 옮기는 것은 이 다음이다(ADR-0118 ⑥).
- 출처: [SSHOperator](https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/),
  [FAB SSO](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/sso.html),
  [OpenLineage Airflow](https://openlineage.io/docs/integrations/airflow/),
  [sshd authorized_keys `command=`·`restrict`](https://man.openbsd.org/sshd#AUTHORIZED_KEYS_FILE_FORMAT).
