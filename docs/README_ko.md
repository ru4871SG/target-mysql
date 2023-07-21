# target-mysql

`target-mysql`은 MySQL을 위한 Singer target이며 [Meltano Target SDK](https://sdk.meltano.com)를 사용하여 빌드되었습니다.

[English](../README.md) | 한국어

## 설치

PIP를 이용한 설치:

```bash
pip install thk-target-mysql
```

GitHub Repo를 이용한 설치:

```bash
pipx install git+https://github.com/thkwag/target-mysql.git@main
```

## 설정

`target-mysql`에서 사용 가능한 설정 옵션들은 다음과 같습니다:

| 설정 옵션                    | 설명                                | 기본값             |
|--------------------------|-----------------------------------|-----------------|
| host                     | MySQL 서버 호스트 이름 또는 IP 주소          |                 |
| port                     | MySQL 서버 실행 포트                    |                 |
| user                     | MySQL 사용자 이름                      |                 |
| password                 | MySQL 사용자 비밀번호                    |                 |
| database                 | MySQL 데이터베이스명                     |                 |
| table_name_pattern       | MySQL 테이블 이름 패턴                   | "${TABLE_NAME}" |
| lower_case_table_names   | 테이블명 소문자 사용 여부                    | true            |
| allow_column_alter       | 컬럼 변경 허용 여부                       | false           |
| replace_null             | null 값을 다른 값으로 대체여부               | false           |

설정은 JSON 형식의 설정 파일저장하고 `target-mysql` 명령을 실행할 때 `--config` 플래그를 사용하여 지정할 수 있습니다.

### `replace_null` 옵션 (실험적)

`replace_null` 옵션을 사용하면, null 값을 데이터 타입에 적합한 '비어있는' 값으로 대체하여 null 값으로 인한 문제를 방지할 수 있습니다. 그러나 이 옵션을 사용할 때는 데이터의 의미가 변경될 수 있으므로 주의가 필요합니다.

`replace_null` 옵션이 `true`일 때 각 JSON Schema 데이터 타입에 대한 null 값의 대체 방식:

| JSON Schema 데이터 타입 | null 값 대체   |
|--------------------|-------------|
| string             | 빈 문자열(`""`) |
| number             | `0`         |
| object             | 빈 객체(`{}`)  |
| array              | 빈 배열(`[]`)  |
| boolean            | `false`     |
| null               | null        |


## 사용법

```bash
cat <input_stream> | target-mysql --config <config.json>
```

- `<input_stream>`: 입력 데이터 스트림
- `<config.json>`: JSON 설정파일

`target-mysql`은 Singer Tap에서 출력된 데이터를 읽고, 이를 MySQL 데이터베이스에 쓰는 역할을 합니다. `target-mysql`을 실행하기 전에 Singer Tap을 실행하여 데이터를 생성해야 합니다.

다음은 Singer Tap과 `target-mysql`을 함께 사용하는 예제입니다:

```bash
tap-exchangeratesapi | target-mysql --config config.json
```

이 예제에서 `tap-exchangeratesapi`는 Singer Tap으로 환율 데이터를 생성합니다. 이 데이터는 파이프(`|`)를 통해 `target-mysql`에 전달되며, `target-mysql`은 이 데이터를 MySQL 데이터베이스에 씁니다. `config.json`은 `target-mysql`의 설정을 담고있는 JSON 파일입니다.


## 개발자 리소스


### 개발 환경 초기화

```bash
pipx install poetry
poetry install
```

### 테스트 생성 및 실행

`target_mysql/tests` 하위 폴더 내에 테스트를 생성하고 다음을 실행합니다:

```bash
poetry run pytest
```

`poetry run`을 사용하여 `target-mysql` CLI 인터페이스를 직접 테스트할 수도 있습니다:

```bash
poetry run target-mysql --help
```

### [Meltano](https://meltano.com/)를 사용한 테스트

_**참고:** 이 target은 Singer 환경에서 작동하며 Meltano가 없어도 동작합니다._

먼저, Meltano를 설치하고 (아직 설치하지 않았다면) 필요한 플러그인을 설치합니다:

```bash
# Meltano 설치
pipx install meltano

# 이 디렉토리에서 Meltano 초기화
cd target-mysql
meltano install
```

이제 Meltano를 사용하여 테스트하고 오케스트레이션할 수 있습니다:

```bash
# 테스트 호출:
meltano invoke target-mysql --version

# 또는 Carbon Intensity 샘플 tap과 함께 run 명령으로 파이프라인 실행:
meltano run tap-carbon-intensity target-mysql
```

### SDK 개발 가이드

Meltano Singer SDK를 사용하여 Singer Taps와 Targets를 개발하는 자세한 지침은 [개발 가이드](https://sdk.meltano.com/en/latest/dev_guide.html)를 참조하십시오.

## 참고 문서 링크

- [Meltano Target SDK Documentation](https://sdk.meltano.com)
- [Singer Specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
- [Meltano](https://meltano.com/)
- [Singer.io](https://www.singer.io/)