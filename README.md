# Milo-Client-Gateway
## OPC UA Client Gateway (MES 연동용)
## 개요
본 레포지토리는 Eclipse Milo 라이브러리를 사용하여 구현한 OPC UA Client 기반 게이트웨이입니다.  
"CtrlLine (MES + ERP 통합 프로젝트)" 에서 MES와 OPC UA 기반 설비(또는 가상 설비) 간의 데이터 연동을  
담당하는 역할로 개발되었습니다.
## 개발목적
- OPC UA 서버(셀지 설비 또는 가상 설비) 와의 통신
- 생산 데이터 수집 및 처리
- MES 상위 시스템으로 데이터 전달
## 주요 기능
- Ecliopse Milo 기반 OPC UA Client 구현
- Subscription 기반 데이터 수신
- 설비 데이터의 주기적 모니터링
- MES 연동을 위한 게이트웨이 구조 설계
## 기술 스택
- Java
- Eclipse Milo (OPC UA)
- OPC UA Client / Subscription 모델
## 활용 시나리오
본 게이트웨이는 "Milo-Server(가상공장)"와 함께 사용되어 MES - 설비 간 End to End 통신 구조를 검증하는 용도로 활용되었습니다.  
실제 설비 연동 전, 데이터 흐름과 시스템 간 연계를 테스트 하는 중간 계층 역할을 수행했습니다.  
## 참고 사항
- 본 프로젝트는 통신 구조 검증및 연동 테스트 목적에 초점을 둔 구현입니다.
- 운영 환경 적용보다는 개발 및 테스트 단계 활용을 전제로 설계되었습니다. 
