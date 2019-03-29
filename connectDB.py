import cx_Oracle as db
import petl as etl

if __name__ == '__main__':

    # Estabelecendo conexao
    # username/password@host/SID
    conexao_op = db.connect("locadora/locadora@localhost:1521/xe")
    conexao_dw = db.connect("dw_locadora/dw_locadora@localhost:1521/xe")

    cursor_op = conexao_op.cursor()
    cursor_dw = conexao_dw.cursor()


    def dm_socio(self):
        tb_socio = cursor_op.execute('select cod_soc, nom_soc , dsc_tps from socios join tipos_socios using(cod_tps);')
        tbs_socio = tb_socio.fetchall()

        for l in tbs_socio:
            cursor_dw.execute("insert into dm_socio(id_soc, nom_soc, tipo_socio) values (:1,:2,:3)", l)


    def dm_artista():
        tb_artista = cursor_op.execute('select cod_art, tpo_art, nac_bras, nom_art from artistas')
        tbs_artista = tb_artista.fetchall()

        for l in tbs_artista:
            cursor_dw.execute("insert into dm_artista(id_art, tpo_art, nac_bras, nom_art) values (:1,:2,:3, :4)", l)


    def dm_titulo():
        tb_titulo = cursor_op.execute('select cod_tit, tpo_tit, cla_tit, dsc_tit from titulos')
        tbs_titulo = tb_titulo.fetchall()

        for l in tbs_titulo:
            cursor_dw.execute("insert into dm_titulo(id_titulo, tpo_titulo, cla_titulo, dsc_titulo) values (:1,:2,:3, :4)",
                              l)


    def dm_gravadora():
        tb_gravadora = cursor_op.execute('select cod_grav, uf_grav, nac_bras, nom_grav from gravadoras')
        tbs_gravadora = tb_gravadora.fetchall()

        for l in tbs_gravadora:
            cursor_dw.execute("insert into dm_gravadora(id_grav, uf_grav, nac_bras, nom_grav) values (:1,:2,:3, :4)", l)


    def dm_tempo():
        tb_tempo = cursor_op.execute(
            """SELECT TO_CHAR(dat_loc, 'YY'), TO_CHAR(dat_loc, 'MM'),TO_CHAR(dat_loc, 'YY/MM'), TO_CHAR(dat_loc, 'MON'),
            TO_CHAR(dat_loc, 'MM/YY'),TO_CHAR(dat_loc, 'fmMONTH'), TO_CHAR(dat_loc, 'DD'), TO_CHAR(dat_loc) ,  
            TO_CHAR(dat_loc, 'HH24'), TO_CHAR(dat_loc, 'AM') FROM  itens_locacoes""")

        tbs_tempo = tb_tempo.fetchall()
        tabela_result = []
        id_tempo = 1
        turno = ''

        for l in tbs_tempo:
            #if(l)
            tabela_result.append([id_tempo, l, turno])
            id_tempo += 1


        for l in tabela_result:
            cursor_dw.execute("insert into dm_tempo(id_tempo, nu_ano, nu_mes, nu_anomes, sg_mes, nm_mesano, nm_mes, "
                              "nu_dia, dt_tempo, nu_hora, turno) "
                              "values (:1,:2,:3, :4, :5, :6, :7, :8, :9, :10, :11)", l)

    def ft_locacoes():
        ft_locacoes = cursor_dw.execute(
            """select id_soc, id_titulo, id_art, id_grav, id_tempo""")

        tbs_locacoes = ft_locacoes.fetchall()

        for l in tbs_locacoes:
            cursor_dw.execute("insert into ft_locacoes(id_grav, uf_grav, nac_bras, nom_grav) values (:1,:2,:3, :4)", l)





    #conexao_dw.commit()
    cursor_dw.close()
    cursor_op.close()
    conexao_dw.close()
    conexao_op.close()